import pandas as pd

OUTDIR = "../data/"

def cm_load_uni(outdir=OUTDIR):
    return pd.read_csv(f"{outdir}/cm_universe/cm_universe_latest.csv")