"""Utility to calculate filter rates from filtered output files."""
from icecube.icetray import I3ConditionalModule, I3Units

from __future__ import annotations
from collections import defaultdict

import Path

class FilterRateMonitorI3Module(I3ConditionalModule):
    """Getting filter rates"""

    def __init__(self, context):
        """Setting things up"""
        I3ConditionalModule.__init__(self, context)
        self.AddParameter("eventheader_key",
                          "I3EventHeader to use",
                          "I3EventHeader")
        self.AddParameter("filtermask_key",
                          "FilterMask to use",
                          "OnlineFilterMask")
        self.AddParameter("output_file",
                          "file to write information to",
                          None)
        self.filter_cnt = defaultdict(int)
        self.start_time = None
        self.stop_time = None
        self.header_cnt = 0
        self.frame_cnt = 0

    def Configure(self):
        """Getting params"""
        self.eventheader_key = self.GetParameter("eventheader_key")
        self.filtermask_key = self.GetParameter("filtermask_key")
        self.outfile = self.GetParameter("output_file")

    def DAQ(self, frame):
        """Counting filters and time for Q frames"""
        if self.eventheader_key in frame:
            self.header_cnt = +1
            if (self.start_time is None) or (self.frame_cnt == 0):
                # Save the first event time
                self.start_time = frame[self.eventheader_key].start_time
            # Save the event time as potential last...
            self.stop_time = frame[self.eventheader_key].start_time
        if self.filtermask_key in frame:
            for name in frame[self.filtermask_key].keys():
                if frame[self.filtermask_key][name].prescale_passed:
                    self.filter_cnt[name] += 1
        self.frame_cnt = +1

    def Finish(self):
        """"Aggregate info and write to txt file"""
        time_l = (self.stop_time - self.start_time) / I3Units.second
        if time_l < 0: 
            raise ValueError("Invalid time length.")
        with Path.open(self.outfile, "w") as f:
            f.write(f"Files cover: {time_l} sec.\n")
            f.write(f"Overall frame rate: {self.frame_cnt / time_l} Hz\n")
            for afilter in self.filter_cnt:
                f.write(f"Filter: {afilter} Rate: {self.filter_cnt[afilter] / time_l} Hz\n")