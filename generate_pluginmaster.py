import json
import os
import requests
from os.path import getmtime
from zipfile import ZipFile
import shutil
from pathlib import Path

BRANCH = os.environ.get("GITHUB_REF", "main").split("refs/heads/")[-1]
DOWNLOAD_URL = "https://github.com/WigglyMuffin/DalamudPlugins/raw/{branch}/plugins/{plugin_name}/latest.zip"
TESTING_DOWNLOAD_URL = "https://github.com/WigglyMuffin/DalamudPlugins/raw/{branch}/plugins/{plugin_name}/testing/latest.zip"
GLOBAL_DOWNLOAD_URL = "https://github.com/WigglyMuffin/DalamudPlugins/raw/{branch}/plugins/{plugin_name}/global/latest.zip"

# External plugins to fetch automatically
# Format: {
#   "plugin_name": {
#     "main": "https://github.com/WigglyMuffin/SamplePlugin/latest.zip", 
#     "testing": "https://github.com/WigglyMuffin/SamplePlugin/testing/latest.zip", #(optional)
#     "global": "https://github.com/WigglyMuffin/SamplePlugin/global/latest.zip" #(optional)
#   },
# }
EXTERNAL_PLUGINS = {
}

DUPLICATES = {
    "DownloadLinkInstall": ["DownloadLinkUpdate"],
}

TRIMMED_KEYS = [
    "Author",
    "Name",
    "Punchline",
    "Description",
    "Tags",
    "InternalName",
    "RepoUrl",
    "Changelog",
    "AssemblyVersion",
    "ApplicableVersion",
    "DalamudApiLevel",
    "TestingAssemblyVersion",
    "TestingDalamudApiLevel",
    "IconUrl",
    "ImageUrls",
]


def main():
    # First, download external plugins
    download_external_plugins()
    
    # Then proceed with existing functionality
    master = extract_manifests()
    master = [trim_manifest(manifest) for manifest in master]
    add_extra_fields(master)
    write_master(master)
    last_update()


def download_external_plugins():
    """Download plugins from external URLs and place them in the proper directories."""
    for plugin_name, urls in EXTERNAL_PLUGINS.items():
        # Create plugin directories if they don't exist
        plugin_dir = Path(f"./plugins/{plugin_name}")
        plugin_dir.mkdir(parents=True, exist_ok=True)
        
        if "main" in urls:
            download_plugin(urls["main"], plugin_dir / "latest.zip")
        
        if "testing" in urls:
            testing_dir = plugin_dir / "testing"
            testing_dir.mkdir(exist_ok=True)
            download_plugin(urls["testing"], testing_dir / "latest.zip")
            
        if "global" in urls:
            global_dir = plugin_dir / "global"
            global_dir.mkdir(exist_ok=True)
            download_plugin(urls["global"], global_dir / "latest.zip")


def download_plugin(url, destination_path):
    """Download a plugin from a URL and save it to the specified path only if newer."""
    try:
        # Check if we already have this file
        if destination_path.exists():
            # Get the ETag or Last-Modified header to check if file has changed
            head_response = requests.head(url)
            head_response.raise_for_status()
            
            etag = head_response.headers.get('ETag')
            last_modified = head_response.headers.get('Last-Modified')
            
            # Read previously saved metadata if it exists
            metadata_file = destination_path.with_suffix('.meta')
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    
                # Skip download if ETag or Last-Modified matches
                if (etag and metadata.get('ETag') == etag) or \
                   (last_modified and metadata.get('Last-Modified') == last_modified):
                    print(f"Skipping {url} - already up to date")
                    return True
        
        print(f"Downloading {url} to {destination_path}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Verify the downloaded ZIP file is valid
        try:
            with ZipFile(destination_path) as z:
                pass  # Just testing if it's a valid ZIP
        except Exception as e:
            print(f"Error: Downloaded file is not a valid ZIP: {e}")
            os.remove(destination_path)
            return False
        
        # Save metadata for future comparisons
        if 'ETag' in response.headers or 'Last-Modified' in response.headers:
            metadata = {
                'ETag': response.headers.get('ETag'),
                'Last-Modified': response.headers.get('Last-Modified')
            }
            with open(destination_path.with_suffix('.meta'), 'w') as f:
                json.dump(metadata, f)
                
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False


def extract_manifests():
    manifests = []
    for dirpath, dirnames, filenames in os.walk("./plugins"):
        if "testing" in dirnames:
            dirnames.remove("testing")
        if "global" in dirnames:
            dirnames.remove("global")
        if "latest.zip" not in filenames:
            continue

        plugin_name = dirpath.split("/")[-1]
        base_zip = f"{dirpath}/latest.zip"

        with ZipFile(base_zip) as z:
            base_manifest = json.loads(z.read(f"{plugin_name}.json").decode("utf-8"))

            testing_zip = f"{dirpath}/testing/latest.zip"
            if os.path.exists(testing_zip):
                with ZipFile(testing_zip) as tz:
                    testing_manifest = json.loads(
                        tz.read(f"{plugin_name}.json").decode("utf-8")
                    )
                    base_manifest["TestingAssemblyVersion"] = testing_manifest.get(
                        "AssemblyVersion"
                    )
                    base_manifest["TestingDalamudApiLevel"] = testing_manifest.get(
                        "DalamudApiLevel"
                    )
            manifests.append(base_manifest)

            global_zip = f"{dirpath}/global/latest.zip"
            if os.path.exists(global_zip):
                with ZipFile(global_zip) as gz:
                    global_manifest = json.loads(
                        gz.read(f"{plugin_name}.json").decode("utf-8")
                    )
                    global_manifest["Name"] = f"{global_manifest['Name']} (API12)"
                    manifests.append(global_manifest)
    return manifests


def add_extra_fields(manifests):
    for manifest in manifests:
        is_global = manifest["Name"].endswith("(API12)")

        if is_global:
            manifest["DownloadLinkInstall"] = GLOBAL_DOWNLOAD_URL.format(
                branch=BRANCH,
                plugin_name=manifest["InternalName"],
            )
        else:
            manifest["DownloadLinkInstall"] = DOWNLOAD_URL.format(
                branch=BRANCH, plugin_name=manifest["InternalName"]
            )

        for src, targets in DUPLICATES.items():
            for target in targets:
                if target not in manifest:
                    manifest[target] = manifest[src]

        if "TestingAssemblyVersion" in manifest and not is_global:
            manifest["DownloadLinkTesting"] = TESTING_DOWNLOAD_URL.format(
                branch=BRANCH, plugin_name=manifest["InternalName"]
            )

        manifest["DownloadCount"] = 0


def write_master(master):
    with open("pluginmaster.json", "w") as f:
        json.dump(master, f, indent=4)


def trim_manifest(plugin):
    return {k: plugin[k] for k in TRIMMED_KEYS if k in plugin}


def last_update():
    with open("pluginmaster.json", encoding="utf-8") as f:
        master = json.load(f)

    for plugin in master:
        if plugin["Name"].endswith("_global"):
            file_path = f"plugins/{plugin['InternalName']}/global/latest.zip"
        else:
            file_path = f"plugins/{plugin['InternalName']}/latest.zip"

        modified = int(getmtime(file_path))
        if "LastUpdate" not in plugin or modified != int(plugin.get("LastUpdate", 0)):
            plugin["LastUpdate"] = str(modified)

    with open("pluginmaster.json", "w", encoding="utf-8") as f:
        json.dump(master, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    main()
