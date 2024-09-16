# To be used with ManGO Ingest !
# Requires exiftool (the Perl based metadata extraction tool) in PATH
# Requires pyexiftool

import json
import pathlib

from exiftool import ExifToolHelper  

# start the exiftool helper, it uses a daemon modes
md_extractor = ExifToolHelper()

def extract(path: str, mode="sidecar", sidecar_ext=".metadata.json"):
    """"""

    # too crude? think (t)wi(c|s)e ;-)
    print(f"exiftool extraction requested from {path} ")
    if path.endswith(sidecar_ext):
        # we do not eat our own dogfood
        return {}
    # we do not want to compromise any calling process
    try:
        metadata = md_extractor.get_metadata(path)
    except Exception as e:
        print(f"Exception in metadata extraction: {e}")
        return {}

    # sidecar mode: 
    if mode == "sidecar":
        pathlib.Path(f"{path}.metadata.json").write_text(json.dumps(metadata[0], indent=2))
        print(f"Exiftool metadata extration in sidecar mode", style="red bold")
        return {}
    if mode == "poorirods":
        return metadata
    return {}
