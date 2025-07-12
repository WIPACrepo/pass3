"""Utility to calculate filter rates from filtered output files."""
# ruff: noqa: T201
import argparse
from typing import cast

from pathlib import Path

from icecube import dataclasses, dataio, icetray  # noqa: F401

def get_rates(infiles: list[str], outfile: str):
    start_time = None
    stop_time = None
    frame_cnt = 0
    header_cnt = 0
    filter_cnt = {}  # type: dict[str,int]
    for file in infiles:
        infile = dataio.I3File(file)
        while infile.more():
            try:
                frame = infile.pop_daq()
            except Exception: # ruff: noqa: BLE001
                break
            frame_cnt += 1
            # handle times
            if "I3EventHeader" in frame:
                header_cnt += 1
                header = cast(dataclasses.I3EventHeader, frame["I3EventHeader"])
                if start_time is None:
                    # Save the first event time
                    start_time = header.start_time
                # Save the event time as potential last...
                stop_time = header.start_time
            if "OnlineFilterMask" in frame:
                fm = cast(dataclasses.I3FilterResultMap, frame["OnlineFilterMask"])
                for name in fm.keys():
                    if fm[name].prescale_passed:
                        if name in filter_cnt:
                            filter_cnt[name] += 1
                        else:
                            filter_cnt[name] = 1

    print(filter_cnt)
    print(frame_cnt)
    print(header_cnt)
    assert start_time is not None  # noqa: S101
    assert stop_time is not None  # noqa: S101
    time_l = (stop_time - start_time) / 1.0E9
    if time_l < 0:
        raise ValueError("Invalid time length, quitting")

    with Path.open(outfile, "w") as f:
        print(f"Files cover: {time_l} sec.")
        f.write(f"Files cover: {time_l}  sec.\n")
        print(f"Overall frame rate: {frame_cnt / time_l}  Hz")
        f.write(f"Overall frame rate: {frame_cnt / time_l}  Hz\n")
        for afilter in filter_cnt:
            count = filter_cnt[afilter]
            print(f"Filter: {afilter} Rate: {count / time_l} Hz")
            f.write(f"Filter: {afilter} Rate: {count / time_l} Hz\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outfile",
                        help="output file to write info to",
                        type=str,
                        required=True)
    parser.add_argument("--infiles",
                        help="input files",
                        nargs="+",
                        required=True)
    args = parser.parse_args()

    get_rates(args.infiles, args.outfile)
