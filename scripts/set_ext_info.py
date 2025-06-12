#!/usr/bin/env python

import pandas as pd
import omero.cli
from omero.model import ExternalInfoI
from omero.rtypes import rlong, rstring

def get_images(conn, container):
    con_type = container.split(':')[0]
    con_id = int(container.split(':')[1])
    image_map = dict()
    container = conn.getObject(con_type, attributes={'id': con_id})
    if con_type == 'Project':
        for dataset in container.listChildren():
            for image in dataset.listChildren():
                image_map[image.getName()] = image
    elif con_type == 'Dataset':
        for image in container.listChildren():
            image_map[image.getName()] = image
    return image_map


def get_external_info(conn, extinfo_id):
    params = omero.sys.ParametersI()
    params.addId(extinfo_id)
    query = """
        select e from ExternalInfo as e
        where e.id = :id
            """
    queryService = conn.getQueryService()
    extinfo = queryService.findByQuery(query, params, {"omero.group": "-1"})
    return extinfo


with omero.cli.cli_login() as c:
    conn = omero.gateway.BlitzGateway(client_obj=c.get_client())
    image_map = get_images(conn, 'Project:3204')
    
    # Read the TSV file
    file_path = '../experimentA/idr0170-experimentA-filePaths.tsv'
    df = pd.read_csv(file_path, sep='\t', header=None)

    # Iterate over rows and use columns as named variables
    for index, row in df.iterrows():
        image_name = row.iloc[2]  # 3rd column (0-based indexing)
        image_path = row.iloc[4]  # 5th column (0-based indexing)
        if image_name not in image_map:
            print(f"Image Name: {image_name} not found")
            continue
        image = image_map[image_name]

        print(f"Image Name: {image_name}")
        print(f"Image Path: {image_path}")
        print(f"Image ID: {image.id}")

        if image.details.externalInfo:
            extinfo = image.details.externalInfo
            extinfo = get_external_info(conn, extinfo.id)
            setattr(extinfo, "entityId", rlong(3))
            setattr(extinfo, "entityType", rstring("com.glencoesoftware.ngff:multiscales"))
            setattr(extinfo, "lsid", rstring(image_path))
            conn.getUpdateService().saveAndReturnObject(extinfo, conn.SERVICE_OPTS)
            print("HAS external info.")
        else:
            extinfo = ExternalInfoI()
            setattr(extinfo, "entityId", rlong(3))
            setattr(extinfo, "entityType", rstring("com.glencoesoftware.ngff:multiscales"))
            setattr(extinfo, "lsid", rstring(image_path))
            extinfo = conn.getUpdateService().saveAndReturnObject(extinfo, conn.SERVICE_OPTS)
            image.details.externalInfo = extinfo
            conn.getUpdateService().saveAndReturnObject(image._obj, conn.SERVICE_OPTS)
            print("CREATED external info.")
        print(f"Set lsid: {extinfo.lsid._val}")
        print("-" * 50)


