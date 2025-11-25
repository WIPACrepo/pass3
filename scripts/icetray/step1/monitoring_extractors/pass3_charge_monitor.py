"""Utility to create histograms of the per DOM charge distribution"""
from icecube.icetray import I3ConditionalModule
from icecube.production_histograms.histograms.histogram import Histogram
from icecube.production_histograms.histogram_modules.histogram_module import HistogramModule
from icecube import dataclasses

from pathlib import Path

from collections import defaultdict

import numpy as np

class ChargeMonitorModule(HistogramModule):
    """Following example of production histograms to create charge 
    histograms"""

    def __init__(self):
        HistogramModule.__init__(self)
        self.frame_key = "CleanSuperDST"
        for string in range(1, 87):
            for om in range(1, 61):
                self.append(
                    Histogram(0, 5, 100, f"Charge {string, om}"))

    def DAQ(self, frame):
        """Getting data for histograms"""
        if self.frame_key in frame:
            for omkey, pulses in frame[self.frame_key]:
                charge = sum([p.charge for p in pulses])
                self.histograms[
                    f"Charge {omkey.string, omkey.om}"].fill(charge)


class ChargeMonitorI3Module(I3ConditionalModule):
    """short module to produce and store charge histograms"""
    
    def __init__(self, context):
        I3ConditionalModule.__init__(self, context)
        self.AddParameter("input_key",
                          "Pulses to monitor",
                          "I3SuperDST")
        self.AddParameter("output_file_path",
                          "npz file to write information to",
                          None)
        self.charges = dict()
        for i in range(86):
            self.charges[i+1] = defaultdict(list)
    
    def Configure(self):
        """Getting variables"""
        self.input_key = self.GetParameter("input_key")
        self.outfile = self.GetParameter("output_file_path")

    def get_charges(self, frame):
        """Get charges from frame"""
        if self.input_key in frame:
            pulsemap = dataclasses.I3RecoPulseSeriesMap.from_frame(
                           frame, self.input_key)
            for omkey in pulsemap:
                charge = sum([p.charge for p in pulsemap[omkey]])
                self.charges[omkey.string][omkey.om].append(charge)

    def DAQ(self, frame):
        """"Getting Q frames"""
        self.get_charges(frame)
        self.PushFrame(frame)

    def Finish(self):
        hists = np.zeros((5160, 100))
        for string in self.charges.keys():
            for om, charges in self.charges[string].items():
                hists[60*(string - 1) + (om - 1)] = np.histogram(charges, bins=100, range=(0., 5.))[0]
        with Path.open(self.outfile, "bw") as fp:
            np.save(fp, hists)
