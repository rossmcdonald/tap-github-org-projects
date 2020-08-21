import argparse
import os
import sys
import json
import collections
import requests
import singer
import singer.bookmarks as bookmarks
import singer.metrics as metrics

from singer import metadata

session = requests.Session()
logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["access_token", "organizations"]

KEY_PROPERTIES = {
    "projects": ["id"],
    "project_columns": ["id"],
    "project_cards": ["id"],
}


class AuthException(Exception):
    pass


class NotFoundException(Exception):
    pass


def translate_state(state, catalog, organizations):
    """
    This tap used to only support a single organization, in which case the
    state took the shape of:
    {
      "bookmarks": {
        "singer-io": {
          "since": "2018-11-14T13:21:20.700360Z"
        }
      }
    }
    The tap now supports multiple repos, so this function should be called
    at the beginning of each run to ensure the state is translate to the
    new format:
    {
      "bookmarks": {
        "singer-io": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
        "stoplightio": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }
    """
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        for org in organizations:
            if bookmarks.get_bookmark(state, org, stream_name):
                return state
            if bookmarks.get_bookmark(state, stream_name, "since"):
                new_state["bookmarks"][org][stream_name][
                    "since"
                ] = bookmarks.get_bookmark(state, stream_name, "since")

    return new_state


def get_bookmark(state, organization, stream_name, bookmark_key):
    organization_stream_dict = bookmarks.get_bookmark(state, organization, stream_name)
    if organization_stream_dict:
        return organization_stream_dict.get(bookmark_key)
    return None


# pylint: disable=dangerous-default-value
def authed_get(source, url, headers={}):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method="get", url=url)
        if resp.status_code == 401:
            raise AuthException(resp.text)
        if resp.status_code == 403:
            raise AuthException(resp.text)
        if resp.status_code == 404:
            raise NotFoundException(resp.text)

        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def authed_get_all_pages(source, url, headers={}):
    while True:
        r = authed_get(source, url, headers)
        r.raise_for_status()
        yield r
        if "next" in r.links:
            url = r.links["next"]["url"]
        else:
            break


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


class DependencyException(Exception):
    pass


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = (
        "Unable to extract {0} data. "
        "To receive {0} data, you also need to select {1}."
    )

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(mdata, values, breadcrumb):
    mdata.append({"metadata": values, "breadcrumb": breadcrumb})


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    # mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(
        mdata, (), "table-key-properties", KEY_PROPERTIES[schema_name]
    )

    for field_name in schema["properties"].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "automatic"
            )
        else:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "available"
            )

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def get_all_projects(schemas, organization, state, mdata):
    bookmark_value = get_bookmark(state, organization, "projects", "since")
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter("projects") as counter:
        # pylint: disable=too-many-nested-blocks
        for response in authed_get_all_pages(
            "projects",
            "https://api.github.com/orgs/{}/projects?sort=created_at&direction=desc".format(
                organization
            ),
            {"Accept": "application/vnd.github.inertia-preview+json"},
        ):
            projects = response.json()
            extraction_time = singer.utils.now()
            for r in projects:
                r["_sdc_organization"] = organization

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if (
                    bookmark_time
                    and singer.utils.strptime_to_utc(r.get("updated_at"))
                    < bookmark_time
                ):
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(
                        r, schemas, metadata=metadata.to_map(mdata)
                    )
                singer.write_record("projects", rec, time_extracted=extraction_time)
                singer.write_bookmark(
                    state,
                    organization,
                    "projects",
                    {"since": singer.utils.strftime(extraction_time)},
                )
                counter.increment()

                project_id = r.get("id")

                # sync project_columns if that schema is present (only there if selected)
                if schemas.get("project_columns"):
                    for project_column_rec in get_all_project_columns(
                        project_id,
                        schemas["project_columns"],
                        organization,
                        state,
                        mdata,
                    ):
                        singer.write_record(
                            "project_columns",
                            project_column_rec,
                            time_extracted=extraction_time,
                        )
                        singer.write_bookmark(
                            state,
                            organization,
                            "project_columns",
                            {"since": singer.utils.strftime(extraction_time)},
                        )

                        # sync project_cards if that schema is present (only there if selected)
                        if schemas.get("project_cards"):
                            column_id = project_column_rec["id"]
                            for project_card_rec in get_all_project_cards(
                                column_id,
                                schemas["project_cards"],
                                organization,
                                state,
                                mdata,
                            ):
                                singer.write_record(
                                    "project_cards",
                                    project_card_rec,
                                    time_extracted=extraction_time,
                                )
                                singer.write_bookmark(
                                    state,
                                    organization,
                                    "project_cards",
                                    {"since": singer.utils.strftime(extraction_time)},
                                )
    return state


def get_all_project_cards(column_id, schemas, organization, state, mdata):
    bookmark_value = get_bookmark(
        state, organization, "project_cards", "{}/since".format(column_id)
    )
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter("project_cards") as counter:
        for response in authed_get_all_pages(
            "project_cards",
            "https://api.github.com/projects/columns/{}/cards?sort=created_at&direction=desc".format(
                column_id
            ),
            {"Accept": "application/vnd.github.inertia-preview+json"},
        ):
            project_cards = response.json()
            for r in project_cards:
                r["_sdc_organization"] = organization

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                t = singer.utils.strptime_to_utc(r.get("updated_at"))
                if bookmark_time and t < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(
                        r, schemas, metadata=metadata.to_map(mdata)
                    )
                counter.increment()
                yield rec

    return state


def get_all_project_columns(project_id, schemas, organization, state, mdata):
    bookmark_value = get_bookmark(state, organization, "project_columns", "since")
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter("project_columns") as counter:
        for response in authed_get_all_pages(
            "project_columns",
            "https://api.github.com/projects/{}/columns?sort=created_at&direction=desc".format(
                project_id
            ),
            {"Accept": "application/vnd.github.inertia-preview+json"},
        ):
            project_columns = response.json()

            for r in project_columns:
                r["_sdc_organization"] = organization

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if (
                    bookmark_time
                    and singer.utils.strptime_to_utc(r.get("updated_at"))
                    < bookmark_time
                ):
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(
                        r, schemas, metadata=metadata.to_map(mdata)
                    )
                counter.increment()
                yield rec

    return state


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


SYNC_FUNCTIONS = {
    "projects": get_all_projects,
}

SUB_STREAMS = {
    "projects": ["project_cards", "project_columns"],
}


def do_sync(config, state, catalog):
    access_token = config["access_token"]
    session.headers.update({"authorization": "token " + access_token})

    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    organizations = list(filter(None, config["organizations"].split(" ")))

    state = translate_state(state, catalog, organizations)
    singer.write_state(state)

    # pylint: disable=too-many-nested-blocks
    for org in organizations:
        logger.info("Starting sync of projects in org: %s", org)
        for stream in catalog["streams"]:
            stream_id = stream["tap_stream_id"]
            stream_schema = stream["schema"]
            mdata = stream["metadata"]

            # if it is a "sub_stream", it will be sync'd by its parent
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema and sync
            if stream_id in selected_stream_ids:
                singer.write_schema(stream_id, stream_schema, stream["key_properties"])

                # get sync function and any sub streams
                sync_func = SYNC_FUNCTIONS[stream_id]
                sub_stream_ids = SUB_STREAMS.get(stream_id, None)

                # sync stream
                if not sub_stream_ids:
                    state = sync_func(stream_schema, org, state, mdata)

                # handle streams with sub streams
                else:
                    stream_schemas = {stream_id: stream_schema}

                    # get and write selected sub stream schemas
                    for sub_stream_id in sub_stream_ids:
                        if sub_stream_id in selected_stream_ids:
                            sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                            stream_schemas[sub_stream_id] = sub_stream["schema"]
                            singer.write_schema(
                                sub_stream_id,
                                sub_stream["schema"],
                                sub_stream["key_properties"],
                            )

                    # sync stream and it's sub streams
                    state = sync_func(stream_schemas, org, state, mdata)

                singer.write_state(state)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
