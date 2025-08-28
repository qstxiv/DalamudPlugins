import json
import os
import requests
from pathlib import Path
from zipfile import ZipFile
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict


@dataclass
class Config:
    """Configuration settings for the plugin master generator."""
    branch: str
    plugins_dir: Path
    output_file: Path
    repository_list: Dict[str, str]
    external_plugins: Dict[str, Dict[str, str]]
    download_urls: Dict[str, str]
    required_manifest_keys: List[str]
    field_duplicates: Dict[str, List[str]]

    @classmethod
    def load_default(cls) -> 'Config':
        """Load default configuration."""
        branch = os.environ.get("GITHUB_REF", "main").split("refs/heads/")[-1]
        base_url = "https://github.com/WigglyMuffin/DalamudPlugins/raw/{branch}/plugins/{plugin_name}"

        repository_list = {
            "Mercury": "https://github.com/WigglyMuffin/MercurysEye",
            "Questionable": "https://github.com/WigglyMuffin/Questionable",
        }

        return cls(
            branch=branch,
            plugins_dir=Path("./plugins"),
            output_file=Path("./pluginmaster.json"),
            repository_list=repository_list,
            external_plugins={},
            download_urls={
                "main": f"{base_url}/latest.zip",
                "testing": f"{base_url}/testing/latest.zip",
                "global": f"{base_url}/global/latest.zip"
            },
            required_manifest_keys=[
                "Author", "Name", "Punchline", "Description", "Tags",
                "InternalName", "RepoUrl", "Changelog", "AssemblyVersion",
                "ApplicableVersion", "DalamudApiLevel", "TestingAssemblyVersion",
                "TestingDalamudApiLevel", "IconUrl", "ImageUrls"
            ],
            field_duplicates={
                "DownloadLinkInstall": ["DownloadLinkUpdate"]
            }
        )


class PluginProcessor:
    """Handles processing of individual plugin manifests."""
    
    def __init__(self, config: Config):
        self.config = config

    def extract_manifest_from_zip(self, zip_path: Path, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Extract and parse manifest from a plugin ZIP file."""
        try:
            with ZipFile(zip_path) as z:
                manifest_data = z.read(f"{plugin_name}.json").decode("utf-8")
                return json.loads(manifest_data)
        except Exception as e:
            print(f"Error reading manifest from {zip_path}: {e}")
            return None

    def process_plugin_directory(self, plugin_dir: Path) -> List[Dict[str, Any]]:
        """Process a single plugin directory and return list of manifests."""
        manifests = []
        plugin_name = plugin_dir.name
        
        main_zip = plugin_dir / "latest.zip"
        if not main_zip.exists():
            return manifests

        base_manifest = self.extract_manifest_from_zip(main_zip, plugin_name)
        if not base_manifest:
            return manifests

        testing_zip = plugin_dir / "testing" / "latest.zip"
        if testing_zip.exists():
            testing_manifest = self.extract_manifest_from_zip(testing_zip, plugin_name)
            if testing_manifest:
                base_manifest["TestingAssemblyVersion"] = testing_manifest.get("AssemblyVersion")
                base_manifest["TestingDalamudApiLevel"] = testing_manifest.get("DalamudApiLevel")

        manifests.append(base_manifest)

        global_zip = plugin_dir / "global" / "latest.zip"
        if global_zip.exists():
            global_manifest = self.extract_manifest_from_zip(global_zip, plugin_name)
            if global_manifest:
                global_manifest["Name"] = f"{global_manifest['Name']} (API13)"
                manifests.append(global_manifest)

        return manifests

    def trim_manifest(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Keep only required keys from manifest."""
        return {k: manifest[k] for k in self.config.required_manifest_keys if k in manifest}

    def add_download_links(self, manifest: Dict[str, Any]) -> None:
        """Add download links and other computed fields to manifest."""
        is_global = manifest["Name"].endswith("(API13)")
        plugin_name = manifest["InternalName"]

        repo_download_url = self._get_repo_download_url(manifest)

        if repo_download_url:
            manifest["DownloadLinkInstall"] = repo_download_url
            print(f"Using repository releases for {plugin_name}: {repo_download_url}")
        else:
            url_key = "global" if is_global else "main"
            manifest["DownloadLinkInstall"] = self.config.download_urls[url_key].format(
                branch=self.config.branch, plugin_name=plugin_name
            )
            print(f"Using local files for {plugin_name}")

        if "TestingAssemblyVersion" in manifest and not is_global:
            manifest["DownloadLinkTesting"] = self.config.download_urls["testing"].format(
                branch=self.config.branch, plugin_name=plugin_name
            )

        # Duplicate fields as configured
        for src, targets in self.config.field_duplicates.items():
            for target in targets:
                if src in manifest and target not in manifest:
                    manifest[target] = manifest[src]

        manifest["DownloadCount"] = 0

    def _get_repo_download_url(self, manifest: Dict[str, Any]) -> Optional[str]:
        """Get download URL from repository releases if available."""
        try:
            repo_url = manifest.get("RepoUrl", "")
            if not repo_url or "github.com" not in repo_url:
                return None

            repo_path = repo_url.replace("https://github.com/", "").rstrip("/")
            if "/" not in repo_path:
                return None

            owner, repo = repo_path.split("/", 1)

            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
            response = requests.get(api_url)

            if response.status_code == 200:
                release_data = response.json()

                plugin_name = manifest["InternalName"]
                assets = release_data.get("assets", [])

                preferred_asset_name = None

                # Priority 1: Look for "latest.zip" (exact match)
                for asset in assets:
                    asset_name = asset.get("name", "")
                    if asset_name == "latest.zip":
                        preferred_asset_name = asset_name
                        break

                # Priority 2: Look for versioned files like "PluginName-version.zip"
                if not preferred_asset_name:
                    for asset in assets:
                        asset_name = asset.get("name", "")
                        if (asset_name.endswith(".zip") and 
                            asset_name.startswith(f"{plugin_name}-") and 
                            not asset_name.endswith("-latest.zip")):
                            preferred_asset_name = asset_name
                            break

                # Priority 3: Look for exact plugin name match "PluginName.zip"
                if not preferred_asset_name:
                    for asset in assets:
                        asset_name = asset.get("name", "")
                        if asset_name == f"{plugin_name}.zip":
                            preferred_asset_name = asset_name
                            break

                # Priority 4: Fall back to any ZIP file
                if not preferred_asset_name:
                    for asset in assets:
                        asset_name = asset.get("name", "")
                        if asset_name.endswith(".zip"):
                            preferred_asset_name = asset_name
                            break

                # If we found a preferred plugin, return stable latest release URL
                if preferred_asset_name:
                    # Use GitHub's latest release download URL pattern that always points to latest
                    stable_url = f"https://github.com/{owner}/{repo}/releases/latest/download/{preferred_asset_name}"
                    return stable_url

            return None

        except Exception as e:
            print(f"Error checking repository releases for {manifest.get('InternalName', 'unknown')}: {e}")
            return None


class RepositoryPluginProcessor:
    """Handles processing plugins directly from GitHub repositories."""
    
    def __init__(self, config: Config):
        self.config = config
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.headers = {"Authorization": f"token {self.github_token}"} if self.github_token else {}

    def get_repository_plugins(self) -> List[Dict[str, Any]]:
        """Get plugin manifests from configured repositories."""
        manifests = []

        for plugin_name, repo_url in self.config.repository_list.items():
            print(f"Processing repository plugin: {plugin_name} from {repo_url}")
            
            repo_manifest = self._get_manifest_from_repository(plugin_name, repo_url)
            if repo_manifest:
                manifests.append(repo_manifest)

        return manifests

    def _get_manifest_from_repository(self, plugin_name: str, repo_url: str) -> Optional[Dict[str, Any]]:
        """Extract manifest from a GitHub repository's latest release."""
        try:
            repo_path = repo_url.replace("https://github.com/", "").rstrip("/")
            if "/" not in repo_path:
                print(f"Invalid repository URL format for {plugin_name}: {repo_url}")
                return None

            owner, repo = repo_path.split("/", 1)

            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
            response = requests.get(api_url, headers=self.headers)

            if response.status_code == 404:
                print(f"Repository {owner}/{repo} not found or private - skipping")
                return None
            elif response.status_code == 403:
                print(f"Access forbidden for {owner}/{repo} (rate limited or private) - skipping")
                return None
            elif response.status_code != 200:
                print(f"Error accessing repository {owner}/{repo}: HTTP {response.status_code}")
                return None

            release_data = response.json()

            release_date = release_data.get("published_at")
            if release_date:
                from datetime import datetime
                try:
                    dt = datetime.fromisoformat(release_date.replace('Z', '+00:00'))
                    release_timestamp = str(int(dt.timestamp()))
                except Exception:
                    release_timestamp = None
            else:
                release_timestamp = None

            plugin_zip_url = self._find_plugin_asset(release_data, plugin_name)
            if not plugin_zip_url:
                print(f"No suitable ZIP asset found for {plugin_name} in {owner}/{repo}")
                return None

            manifest = self._extract_manifest_from_url(plugin_zip_url, plugin_name)
            if manifest:
                manifest["RepoUrl"] = repo_url
                manifest["_repository_source"] = True
                if release_timestamp:
                    manifest["LastUpdate"] = release_timestamp
                print(f"Successfully extracted manifest for {plugin_name} v{manifest.get('AssemblyVersion', 'unknown')}")
                return manifest

        except Exception as e:
            print(f"Error processing repository plugin {plugin_name}: {e}")
            return None

        return None

    def _find_plugin_asset(self, release_data: Dict[str, Any], plugin_name: str) -> Optional[str]:
        """Find the best plugin ZIP asset from release assets."""
        assets = release_data.get("assets", [])

        repo_info = release_data.get("html_url", "")
        # Extract base repo URL from release URL
        if "/releases/tag/" in repo_info:
            repo_url = repo_info.split("/releases/tag/")[0]
        else:
            # Fallback: try to get from repository field
            repo_data = release_data.get("repository", {})
            repo_url = repo_data.get("html_url", "")

        if not repo_url:
            return None

        # Priority 1: Look for "latest.zip"
        for asset in assets:
            if asset.get("name") == "latest.zip":
                return f"{repo_url}/releases/latest/download/latest.zip"

        # Priority 2: Look for exact plugin name match
        for asset in assets:
            if asset.get("name") == f"{plugin_name}.zip":
                return f"{repo_url}/releases/latest/download/{plugin_name}.zip"

        # Priority 3: Look for versioned files
        for asset in assets:
            asset_name = asset.get("name", "")
            if asset_name.endswith(".zip") and asset_name.startswith(f"{plugin_name}-"):
                return f"{repo_url}/releases/latest/download/{asset_name}"

        # Priority 4: Any ZIP file
        for asset in assets:
            if asset.get("name", "").endswith(".zip"):
                asset_name = asset.get("name")
                return f"{repo_url}/releases/latest/download/{asset_name}"

        return None

    def _extract_manifest_from_url(self, zip_url: str, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Download ZIP file and extract plugin manifest."""
        try:
            response = requests.get(zip_url, stream=True)
            response.raise_for_status()

            temp_zip_path = Path(f"temp_{plugin_name}.zip")
            try:
                with open(temp_zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                with ZipFile(temp_zip_path) as z:
                    manifest_data = z.read(f"{plugin_name}.json").decode("utf-8")
                    return json.loads(manifest_data)

            finally:
                # Clean up temporary file
                if temp_zip_path.exists():
                    temp_zip_path.unlink()

        except Exception as e:
            print(f"Error extracting manifest from {zip_url}: {e}")
            return None


class ExternalPluginManager:
    """Handles downloading and caching of external plugins."""

    def __init__(self, config: Config):
        self.config = config

    def download_external_plugins(self) -> None:
        """Download all configured external plugins."""
        for plugin_name, urls in self.config.external_plugins.items():
            plugin_dir = self.config.plugins_dir / plugin_name
            plugin_dir.mkdir(parents=True, exist_ok=True)

            for variant, url in urls.items():
                if variant == "main":
                    dest_path = plugin_dir / "latest.zip"
                else:
                    variant_dir = plugin_dir / variant
                    variant_dir.mkdir(exist_ok=True)
                    dest_path = variant_dir / "latest.zip"

                self._download_if_needed(url, dest_path)

    def _download_if_needed(self, url: str, dest_path: Path) -> bool:
        """Download file only if it's newer than local copy."""
        try:
            # Check if we should skip download based on metadata
            if self._is_up_to_date(url, dest_path):
                print(f"Skipping {url} - already up to date")
                return True

            print(f"Downloading {url} to {dest_path}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            with ZipFile(dest_path) as z:
                pass

            self._save_metadata(response, dest_path)
            return True

        except Exception as e:
            print(f"Error downloading {url}: {e}")
            if dest_path.exists():
                dest_path.unlink()
            return False

    def _is_up_to_date(self, url: str, dest_path: Path) -> bool:
        """Check if local file is up to date based on HTTP headers."""
        if not dest_path.exists():
            return False

        try:
            head_response = requests.head(url)
            head_response.raise_for_status()

            metadata_file = dest_path.with_suffix('.meta')
            if not metadata_file.exists():
                return False

            with open(metadata_file, 'r') as f:
                metadata = json.load(f)

            etag = head_response.headers.get('ETag')
            last_modified = head_response.headers.get('Last-Modified')

            return ((etag and metadata.get('ETag') == etag) or
                    (last_modified and metadata.get('Last-Modified') == last_modified))

        except Exception:
            return False

    def _save_metadata(self, response: requests.Response, dest_path: Path) -> None:
        """Save HTTP metadata for future comparison."""
        metadata = {
            'ETag': response.headers.get('ETag'),
            'Last-Modified': response.headers.get('Last-Modified')
        }
        if any(metadata.values()):
            with open(dest_path.with_suffix('.meta'), 'w') as f:
                json.dump(metadata, f)


class DownloadCountUpdater:
    """Handles updating download counts from GitHub releases."""

    def __init__(self):
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.headers = {"Authorization": f"token {self.github_token}"} if self.github_token else {}
        self.repo_cache = {}

    def update_download_counts(self, manifests: List[Dict[str, Any]]) -> None:
        """Update download counts for all manifests."""
        for manifest in manifests:
            try:
                repo_url = manifest.get("RepoUrl", "")
                if not repo_url or "github.com" not in repo_url:
                    continue

                owner, repo = self._parse_github_url(repo_url)
                if not owner or not repo:
                    continue

                repo_key = f"{owner}/{repo}"

                if repo_key not in self.repo_cache:
                    self.repo_cache[repo_key] = self._fetch_download_count(owner, repo)

                manifest["DownloadCount"] = self.repo_cache[repo_key]
                print(f"Updated {manifest['InternalName']}: {manifest['DownloadCount']} downloads")

            except Exception as e:
                print(f"Error updating download count for {manifest.get('InternalName', 'unknown')}: {e}")

    def _parse_github_url(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse GitHub URL to extract owner and repo."""
        try:
            repo_path = url.replace("https://github.com/", "").rstrip("/")
            if "/" in repo_path:
                return repo_path.split("/", 1)
        except Exception:
            pass
        return None, None

    def _fetch_download_count(self, owner: str, repo: str) -> int:
        """Fetch total download count for a GitHub repository."""
        try:
            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases"
            print(f"Fetching download counts for {owner}/{repo}")

            response = requests.get(api_url, headers=self.headers)

            # Handle different HTTP status codes
            if response.status_code == 404:
                print(f"Repository {owner}/{repo} not found or is private - skipping download count")
                return 0
            elif response.status_code == 403:
                print(f"Access forbidden for {owner}/{repo} (rate limited or private) - skipping download count")
                return 0
            elif response.status_code == 401:
                print(f"Authentication required for {owner}/{repo} - skipping download count")
                return 0

            response.raise_for_status()

            releases = response.json()

            # Handle case where repository has no releases
            if not releases:
                print(f"Repository {owner}/{repo} has no releases")
                return 0

            total_downloads = 0
            for release in releases:
                for asset in release.get("assets", []):
                    total_downloads += asset.get("download_count", 0)
            return total_downloads

        except requests.exceptions.RequestException as e:
            print(f"Network error fetching download count for {owner}/{repo}: {e}")
            return 0
        except Exception as e:
            print(f"Unexpected error fetching download count for {owner}/{repo}: {e}")
            return 0


class PluginMasterGenerator:
    """Main class that orchestrates the plugin master generation process."""

    def __init__(self, config: Config):
        self.config = config
        self.processor = PluginProcessor(config)
        self.repo_processor = RepositoryPluginProcessor(config)
        self.external_manager = ExternalPluginManager(config)
        self.download_updater = DownloadCountUpdater()

    def generate(self) -> None:
        """Generate the plugin master file."""
        print("Starting plugin master generation...")

        if self.config.external_plugins:
            print("Downloading external plugins...")
            self.external_manager.download_external_plugins()

        print("Collecting plugin manifests...")
        manifests = self._collect_manifests_with_priority()

        manifests = [self.processor.trim_manifest(m) for m in manifests]

        for manifest in manifests:
            self.processor.add_download_links(manifest)

        print("Updating download counts...")
        self.download_updater.update_download_counts(manifests)

        print("Writing plugin master file...")
        self._write_plugin_master(manifests)

        self._update_last_modified(manifests)

        print(f"Generated plugin master with {len(manifests)} plugins")

    def _collect_manifests_with_priority(self) -> List[Dict[str, Any]]:
        """Collect plugin manifests with repository-first priority system."""
        manifests = []
        processed_plugins = set()

        # Priority 1: Process repository list first
        print("Processing repository-configured plugins...")
        repo_manifests = self.repo_processor.get_repository_plugins()

        for manifest in repo_manifests:
            plugin_name = manifest.get("InternalName")
            if plugin_name:
                # Check if local version exists for comparison
                local_manifest = self._get_local_manifest(plugin_name)
                
                if local_manifest:
                    # Compare versions and choose the better one
                    chosen_manifest = self._choose_better_manifest(repo_manifest=manifest, local_manifest=local_manifest, plugin_name=plugin_name)
                    manifests.append(chosen_manifest)
                else:
                    # No local version, use repository version
                    print(f"Using repository version for {plugin_name} (no local version found)")
                    manifests.append(manifest)

                processed_plugins.add(plugin_name)

        # Priority 2: Process remaining local plugins not in repository list
        print("Processing remaining local plugins...")
        local_manifests = self._collect_local_manifests()
        
        for manifest in local_manifests:
            plugin_name = manifest.get("InternalName")
            if plugin_name and plugin_name not in processed_plugins:
                print(f"Using local version for {plugin_name} (not in repository list)")
                manifests.append(manifest)
                processed_plugins.add(plugin_name)

        return manifests

    def _get_local_manifest(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Get manifest from local plugins directory."""
        plugin_dir = self.config.plugins_dir / plugin_name
        if plugin_dir.exists() and plugin_dir.is_dir():
            local_manifests = self.processor.process_plugin_directory(plugin_dir)
            return local_manifests[0] if local_manifests else None
        return None

    def _collect_local_manifests(self) -> List[Dict[str, Any]]:
        """Collect all plugin manifests from the local plugins directory."""
        manifests = []

        if not self.config.plugins_dir.exists():
            print(f"Plugins directory {self.config.plugins_dir} does not exist")
            return manifests

        for plugin_dir in self.config.plugins_dir.iterdir():
            if plugin_dir.is_dir():
                plugin_manifests = self.processor.process_plugin_directory(plugin_dir)
                manifests.extend(plugin_manifests)

        return manifests

    def _choose_better_manifest(self, repo_manifest: Dict[str, Any], local_manifest: Dict[str, Any], plugin_name: str) -> Dict[str, Any]:
        """Choose between repository and local manifest based on version comparison."""
        repo_version = repo_manifest.get("AssemblyVersion", "0.0.0")
        local_version = local_manifest.get("AssemblyVersion", "0.0.0")

        print(f"Comparing versions for {plugin_name}: repo={repo_version}, local={local_version}")

        # If versions are the same, prioritise repository
        if repo_version == local_version:
            print(f"Versions are equal for {plugin_name}, prioritising repository version")
            return repo_manifest

        # Compare versions
        try:
            repo_parts = [int(x) for x in repo_version.split('.')]
            local_parts = [int(x) for x in local_version.split('.')]

            # Pad shorter version with zeros
            max_len = max(len(repo_parts), len(local_parts))
            repo_parts.extend([0] * (max_len - len(repo_parts)))
            local_parts.extend([0] * (max_len - len(local_parts)))

            if repo_parts >= local_parts:
                print(f"Repository version is newer or equal for {plugin_name}, using repository")
                return repo_manifest
            else:
                print(f"Local version is newer for {plugin_name}, using local")
                return local_manifest

        except ValueError:
            # If version parsing fails, prioritise repository
            print(f"Could not parse versions for {plugin_name}, prioritising repository")
            return repo_manifest

    def _write_plugin_master(self, manifests: List[Dict[str, Any]]) -> None:
        """Write the plugin master JSON file."""
        with open(self.config.output_file, 'w', encoding='utf-8') as f:
            json.dump(manifests, f, indent=4, ensure_ascii=False)

    def _update_last_modified(self, manifests: List[Dict[str, Any]]) -> None:
        """Update LastUpdate timestamps based on file modification times or repository release dates."""
        for manifest in manifests:
            try:
                plugin_name = manifest["InternalName"]
                
                # Check if this is a repository-sourced plugin
                if manifest.get("_repository_source"):
                    # Repository plugins already have LastUpdate from release date
                    # Remove the temporary marker
                    del manifest["_repository_source"]
                    if "LastUpdate" not in manifest:
                        # Fallback: try to get timestamp from local file
                        self._set_local_timestamp(manifest, plugin_name)
                else:
                    # Local plugin - use file modification time
                    self._set_local_timestamp(manifest, plugin_name)
                    
            except Exception as e:
                print(f"Error updating last modified time for {manifest.get('InternalName', 'unknown')}: {e}")

        # Rewrite the file with updated timestamps
        self._write_plugin_master(manifests)

    def _set_local_timestamp(self, manifest: Dict[str, Any], plugin_name: str) -> None:
        """Set timestamp from local file modification time."""
        is_global = manifest["Name"].endswith("(API13)")
        
        if is_global:
            zip_path = self.config.plugins_dir / plugin_name / "global" / "latest.zip"
        else:
            zip_path = self.config.plugins_dir / plugin_name / "latest.zip"

        if zip_path.exists():
            modified_time = str(int(zip_path.stat().st_mtime))
            manifest["LastUpdate"] = modified_time
        else:
            # If no local file exists, set a default timestamp
            import time
            manifest["LastUpdate"] = str(int(time.time()))


def main():
    """Main entry point."""
    config = Config.load_default()
    generator = PluginMasterGenerator(config)
    generator.generate()


if __name__ == "__main__":
    main()
