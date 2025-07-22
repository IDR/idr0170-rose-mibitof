import logging
import os
import omero
from omero.cli import cli_login
from omero.gateway import BlitzGateway
from omero_rois import mask_from_binary_image
from omero.rtypes import rstring
import zarr
import pandas as pd


PROJECT = "idr0170-rose-mibitof/experimentA"
RGBA = (255, 255, 0, 128)


def get_images(conn):
    project = conn.getObject('Project', attributes={'name': PROJECT})
    for dataset in project.listChildren():
        for image in dataset.listChildren():
            yield dataset, image


def save_roi(conn, im, roi):
    logging.info(f"Saving ROI for image {im.id}:{im.name}")
    us = conn.getUpdateService()
    im = conn.getObject('Image', im.id)
    roi.setImage(im._obj)
    roi = us.saveAndReturnObject(roi)
    if not roi:
        logging.warning("Saving ROI failed.")


def delete_rois(conn, im):
    result = conn.getRoiService().findByImage(im.id, None)
    to_delete = []
    for roi in result.rois:
        to_delete.append(roi.getId().getValue())
    if to_delete:
        logging.info(f"Deleting existing {len(to_delete)} rois")
        conn.deleteObjects("Roi", to_delete, deleteChildren=True, wait=True)


def create_roi(mask_data):
    logging.info(f"Creating ROI for mask data {mask_data.shape}")
    mask = mask_from_binary_image(mask_data[0][0][0] > 0, RGBA, None, None, None, "Tumor", True)
    roi = omero.model.RoiI()
    roi.setName(rstring("Tumor"))
    roi.addShape(mask)
    return roi


def get_paths():
    path = "../experimentA/idr0170-experimentA-filePaths.tsv"
    df = pd.read_csv(path, sep='\t', header=None)
    paths = dict()
    for _, row in df.iterrows():
        dataset_name = row.iloc[0]
        dataset_name = dataset_name.replace("Dataset:name:", "")
        image_name = row.iloc[2]
        paths[f"{dataset_name}|{image_name}"] = row.iloc[4]
    return paths


def get_mask_data(paths, dataset_name, image_name):
    path_to_zarr = paths[f"{dataset_name}|{image_name}"]
    labels_path = f"{path_to_zarr}/labels/tumor/0"
    if os.path.exists(labels_path):
        return zarr.open(labels_path)
    else:
        logging.warning(f"Could not find {labels_path} for {dataset_name}|{image_name}")
        return None


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s [%(pathname)s, %(lineno)s]')
    paths = get_paths()
    with cli_login() as c:
        conn = BlitzGateway(client_obj=c.get_client())
        for ds, im in get_images(conn):
            try:
                logging.info(f"Processing Image: {im.id} {im.name}")
                delete_rois(conn, im)
                mask_data = get_mask_data(paths, ds.name, im.name)
                roi = create_roi(mask_data)
                save_roi(conn, im, roi)
            except Exception as e:
                logging.warning(e)


if __name__ == "__main__":
    main()
