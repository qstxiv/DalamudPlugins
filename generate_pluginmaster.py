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
    external_plugins: Dict[str, Dict[str, str]]
    download_urls: Dict[str, str]
    required_manifest_keys: List[str]
    field_duplicates: Dict[str, List[str]]

    @classmethod
    def load_default(cls) -> 'Config':
        """Load default configuration."""
        branch = os.environ.get("GITHUB_REF", "main").split("refs/heads/")[-1]
        base_url = "https://github.com/WigglyMuffin/DalamudPlugins/raw/{branch}/plugins/{plugin_name}"
        
        return cls(
            branch=branch,
            plugins_dir=Path("./plugins"),
            output_file=Path("./pluginmaster.json"),
            external_plugins={},  # Can be configured as needed
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
        
        # Process main plugin
        main_zip = plugin_dir / "latest.zip"
        if not main_zip.exists():
            return manifests

        base_manifest = self.extract_manifest_from_zip(main_zip, plugin_name)
        if not base_manifest:
            return manifests

        # Add testing version info if available
        testing_zip = plugin_dir / "testing" / "latest.zip"
        if testing_zip.exists():
            testing_manifest = self.extract_manifest_from_zip(testing_zip, plugin_name)
            if testing_manifest:
                base_manifest["TestingAssemblyVersion"] = testing_manifest.get("AssemblyVersion")
                base_manifest["TestingDalamudApiLevel"] = testing_manifest.get("DalamudApiLevel")

        manifests.append(base_manifest)

        # Process global version if available
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
        
        # Check if plugin repository has releases and prefer those
        repo_download_url = self._get_repo_download_url(manifest)
        
        if repo_download_url:
            # Use repository releases for download links
            manifest["DownloadLinkInstall"] = repo_download_url
            print(f"Using repository releases for {plugin_name}: {repo_download_url}")
        else:
            # Fallback to local files
            url_key = "global" if is_global else "main"
            manifest["DownloadLinkInstall"] = self.config.download_urls[url_key].format(
                branch=self.config.branch, plugin_name=plugin_name
            )
            print(f"Using local files for {plugin_name}")

        # Add testing download link if testing version exists
        if "TestingAssemblyVersion" in manifest and not is_global:
            manifest["DownloadLinkTesting"] = self.config.download_urls["testing"].format(
                branch=self.config.branch, plugin_name=plugin_name
            )

        # Duplicate fields as configured
        for src, targets in self.config.field_duplicates.items():
            for target in targets:
                if src in manifest and target not in manifest:
                    manifest[target] = manifest[src]

        # Initialize download count
        manifest["DownloadCount"] = 0

    def _get_repo_download_url(self, manifest: Dict[str, Any]) -> Optional[str]:
        """Get download URL from repository releases if available."""
        try:
            repo_url = manifest.get("RepoUrl", "")
            if not repo_url or "github.com" not in repo_url:
                return None

            # Parse GitHub URL
            repo_path = repo_url.replace("https://github.com/", "").rstrip("/")
            if "/" not in repo_path:
                return None

            owner, repo = repo_path.split("/", 1)
            
            # Check if repository has releases
            api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
            response = requests.get(api_url)
            
            if response.status_code == 200:
                release_data = response.json()
                
                # Look for the plugin ZIP file in assets with priority order
                plugin_name = manifest["InternalName"]
                assets = release_data.get("assets", [])
                
                # Priority 1: Look for "latest.zip" (exact match)
                for asset in assets:
                    asset_name = asset.get("name", "")
                    if asset_name == "latest.zip":
                        return asset.get("browser_download_url")
                
                # Priority 2: Look for exact plugin name match "PluginName.zip"
                for asset in assets:
                    asset_name = asset.get("name", "")
                    if asset_name == f"{plugin_name}.zip":
                        return asset.get("browser_download_url")
                
                # Priority 3: Look for versioned files like "PluginName-version.zip"
                for asset in assets:
                    asset_name = asset.get("name", "")
                    if (asset_name.endswith(".zip") and 
                        asset_name.startswith(f"{plugin_name}-") and 
                        not asset_name.endswith("-latest.zip")):
                        return asset.get("browser_download_url")
                
                # Priority 4: Fall back to any ZIP file
                for asset in assets:
                    asset_name = asset.get("name", "")
                    if asset_name.endswith(".zip"):
                        return asset.get("browser_download_url")

            return None

        except Exception as e:
            print(f"Error checking repository releases for {manifest.get('InternalName', 'unknown')}: {e}")
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

            # Download and verify
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Verify ZIP is valid
            with ZipFile(dest_path) as z:
                pass

            # Save metadata for future checks
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
        self.external_manager = ExternalPluginManager(config)
        self.download_updater = DownloadCountUpdater()

    def generate(self) -> None:
        """Generate the plugin master file."""
        print("Starting plugin master generation...")
        
        # Download external plugins
        if self.config.external_plugins:
            print("Downloading external plugins...")
            self.external_manager.download_external_plugins()

        # Process all plugin directories
        print("Processing plugin directories...")
        manifests = self._collect_manifests()
        
        # Trim manifests to required keys
        manifests = [self.processor.trim_manifest(m) for m in manifests]
        
        # Add computed fields
        for manifest in manifests:
            self.processor.add_download_links(manifest)
        
        # Update download counts
        print("Updating download counts...")
        self.download_updater.update_download_counts(manifests)
        
        # Write output
        print("Writing plugin master file...")
        self._write_plugin_master(manifests)
        
        # Update last modification times
        self._update_last_modified(manifests)
        
        print(f"Generated plugin master with {len(manifests)} plugins")

    def _collect_manifests(self) -> List[Dict[str, Any]]:
        """Collect all plugin manifests from the plugins directory."""
        manifests = []
        
        if not self.config.plugins_dir.exists():
            print(f"Plugins directory {self.config.plugins_dir} does not exist")
            return manifests

        for plugin_dir in self.config.plugins_dir.iterdir():
            if plugin_dir.is_dir():
                plugin_manifests = self.processor.process_plugin_directory(plugin_dir)
                manifests.extend(plugin_manifests)
        
        return manifests

    def _write_plugin_master(self, manifests: List[Dict[str, Any]]) -> None:
        """Write the plugin master JSON file."""
        with open(self.config.output_file, 'w', encoding='utf-8') as f:
            json.dump(manifests, f, indent=4, ensure_ascii=False)

    def _update_last_modified(self, manifests: List[Dict[str, Any]]) -> None:
        """Update LastUpdate timestamps based on file modification times."""
        for manifest in manifests:
            try:
                plugin_name = manifest["InternalName"]
                is_global = manifest["Name"].endswith("(API13)")
                
                if is_global:
                    zip_path = self.config.plugins_dir / plugin_name / "global" / "latest.zip"
                else:
                    zip_path = self.config.plugins_dir / plugin_name / "latest.zip"
                
                if zip_path.exists():
                    modified_time = str(int(zip_path.stat().st_mtime))
                    manifest["LastUpdate"] = modified_time
                    
            except Exception as e:
                print(f"Error updating last modified time for {manifest.get('InternalName', 'unknown')}: {e}")

        # Rewrite the file with updated timestamps
        self._write_plugin_master(manifests)


def main():
    """Main entry point."""
    config = Config.load_default()
    generator = PluginMasterGenerator(config)
    generator.generate()


if __name__ == "__main__":
    main()
