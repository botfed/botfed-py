import os


def check_file(outfile, interval_min=1, min_lines=2):
    if os.path.exists(outfile):
        with open(outfile, "r") as f:
            num_lines = len(f.readlines())
            if min_lines > 0 and num_lines >= min_lines:
                return True
            elif num_lines == 24 * 60 // interval_min + 1:
                return True
    return False


def ticker_outpath(ticker, sdate, outdir):
    return f"{outdir}/{ticker.replace('/','')}/{sdate.strftime('%Y-%m-%d')}.csv"

    
