"""
Convert npz files to json for score-audience predictor

usage example:
    python2.7 score_audience/npz_to_json.py /data/scoring/py_models /data/scoring/scala_apply_models

For example, if there stored file /data/scoring/py_models/vtb.npz
then output should be /data/scoring/scala_apply_models/vtb/model_weights.json

Attention! Before writing output file, its directory will be recreated!
In our example, directory vtb will be removed and then created.
"""

import os
import sys
import json
import numpy as np

import tempfile
import shutil

from luigi.contrib.hdfs import get_autoconfig_client

MODEL_FILENAME = "model_weights.json"


def log(msg, *args):
    print("npz_to_json: " + msg.format(*args))


def read_predictor_npz(path):
    np_obj = np.load(path, allow_pickle=True)

    # file content keys
    t_idf_name = "t_idf"
    g_idf_name = "g_idf"
    w_name = "W"
    b_name = "b"
    d_idf_name = "d_idf"

    # decode nullable fields
    t_idf = np_obj[t_idf_name] if ((t_idf_name in np_obj.keys()) and (np_obj[t_idf_name].ndim != 0)) else None
    g_idf = np_obj[g_idf_name] if ((g_idf_name in np_obj.keys()) and (np_obj[g_idf_name].ndim != 0)) else None

    return {
        w_name:        np_obj[w_name].tolist(),                           # ndarray size 204
        b_name:        np_obj[b_name].ravel()[0],                         # float
        d_idf_name:    np_obj[d_idf_name].tolist(),                       # ndarray size 200
        t_idf_name:    t_idf.tolist() if t_idf else None,
        g_idf_name:    g_idf.tolist() if g_idf else None,
    }


def write_predictor_json(predictor, path):
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as e:        
        if e.errno != 17: # 17: file exists
            log("write_predictor_json, mkdirs failed: {}", e)

    try:
        with open(path, "w") as fh:
            json.dump(predictor, fh, separators=(",", ":"))
    except (IOError, UnicodeError) as e:
        log("write_predictor_json failed: {}", e)
        return False
    return True


def convert_all_npz_in_dir(src_dir, trg_dir):
    res = True
    file_names = os.listdir(src_dir)
    log("src files: '{}'", file_names)

    for fn in file_names:
        if fn.endswith(".npz"):
            npz_path = os.path.join(src_dir, fn)
            log("loading npz '{}' ...", npz_path)
            predictor = read_predictor_npz(npz_path)

            model_name = fn.replace(".npz", "")
            json_path = os.path.join(trg_dir, model_name, MODEL_FILENAME)
            log("writing json '{}' ...", json_path)
            is_written = write_predictor_json(predictor, json_path)
            if is_written:
                log("model '{}' converted successfully", model_name)
            else:
                res = False
                log("model '{}' conversion failed")

    return res


def hdfs_put_from_local_dir(hdfs_client, local_src_dir, trg_dir):
    hdfs_client.mkdir(trg_dir, parents=True, raise_if_exists=False)

    content = os.listdir(local_src_dir)
    for item in content:
        try:
            hdfs_client.remove(os.path.join(trg_dir, item), recursive=True, skip_trash=True)
        except Exception as e:
            log("can't delete old item: '{}'", e)

        item_src_path = os.path.join(local_src_dir, item)
        log("putting files to hdfs, from '{}' to '{}'", item_src_path, trg_dir)
        hdfs_client.put(item_src_path, trg_dir)


def convert_all_npz_in_hdfs_dir(src_dir, trg_dir, hdfs_client):
    local_src_dir = tempfile.mkdtemp("src")
    local_trg_dir = tempfile.mkdtemp("trg")
    try:
        log("getting files from hdfs '{}' to local '{}'", src_dir, local_src_dir)
        hdfs_client.get(src_dir, local_src_dir)

        res = convert_all_npz_in_dir(
            os.path.join(local_src_dir, os.path.basename(src_dir)),
            local_trg_dir
        )

        log("putting files from local '{}' to hdfs '{}'", local_trg_dir, trg_dir)
        hdfs_put_from_local_dir(hdfs_client, local_trg_dir, trg_dir)
    finally:
        shutil.rmtree(local_trg_dir, ignore_errors=True)
        shutil.rmtree(local_src_dir, ignore_errors=True)

    return res


def convert_all_hdfs_files(src_dir, trg_dir):
    hdfs_client = get_autoconfig_client()

    if not convert_all_npz_in_hdfs_dir(src_dir, trg_dir, hdfs_client):
        raise RuntimeError("can't convert files in hdfs directory")
    log("SUCCESS: models from '{}' converted and stored to '{}'", src_dir, trg_dir)


def main():
    convert_all_hdfs_files(sys.argv[1], sys.argv[2])


def test():
    def convert_one_file():
        npz_path = "/tmp/p_hmc_660.npz"
        json_path = "/tmp/p_hmc_660/{}".format(MODEL_FILENAME)
        predictor = read_predictor_npz(npz_path)
        log("predictor loaded from npz '{}', keys: '{}'", npz_path, [k for k, v in predictor.items()])
        is_written = write_predictor_json(predictor, json_path)
        if is_written:
            log("predictor written to json '{}'", json_path)
        else:
            log("write to json '{}' failed", json_path)

    def convert_all_files():
        src_dir = "/tmp"
        trg_dir = "/tmp/score-audience"
        if not convert_all_npz_in_dir(src_dir, trg_dir):
            raise RuntimeError("can't convert files in directory")

    convert_one_file()
    convert_all_files()

    convert_all_hdfs_files(
        "/user/vlk/dm-6260/models",
        "/user/vlk/dm-6260/sa_models"
    )


if __name__ == "__main__":
    if len(sys.argv) > 2:
        main()
    else:
        test()
