# tap-github-org-projects

> This is a forked and modified version of
[tap-github](https://github.com/singer-io/tap-github), but adapted for projects
at the Github organization scope.

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the GitHub API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from the [GitHub REST API](https://developer.github.com/v3/)
- Extracts the following resources from GitHub for a single repository:
  - [Projects](https://docs.github.com/en/rest/reference/projects)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

   We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install < requirements.txt
    ```

2. Create a GitHub access token

    Login to your GitHub account, go to the
    [Personal Access Tokens](https://github.com/settings/tokens) settings
    page, and generate a new token with at least the `repo` scope. Save this
    access token, you'll need it for the next step.

3. Create the config file

    Create a JSON file containing the access token you just created
    and the path to one or multiple organizations that you want to extract data from. Each org path should be space delimited. The org path is relative to
    `https://github.com/`.

    ```json
    {"access_token": "your-access-token",
     "organizations": "someorg another-org"}
    ```
4. Run the tap in discovery mode to get properties.json file

    ```bash
    python3 tap_github/__init__.py --config config.json --discover > properties.json
    ```
5. In the properties.json file, select the streams to sync

    Each stream in the properties.json file has a "schema" entry.  To select a stream to sync, add `"selected": true` to that stream's "schema" entry.  For example, to sync the pull_requests stream:
    ```
    ...
    "tap_stream_id": "pull_requests",
    "schema": {
      "selected": true,
      "properties": {
        "updated_at": {
          "format": "date-time",
          "type": [
            "null",
            "string"
          ]
        }
    ...
    ```

6. Run the application

    `tap-github` can be run with:

    ```bash
    python3 tap_github/__init__.py --config config.json --properties properties.json

    python3 tap_github/__init__.py -c config.json -p properties.json | target-stitch --config stitch.json
    ```

---

Copyright &copy; 2018 Stitch, 2020 Ross McDonald
