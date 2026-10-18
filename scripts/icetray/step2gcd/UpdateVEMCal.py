# --------------------------------------------
# Replace the VEMCal stuff in a GCD file, with stuff from an .xml file on disk
# Write the result to a new GCD file
# This module stolen/adapted from:
# ${I3_SRC}/icetop_Level3_scripts/python/modules/UpdateVEMCal.py
# Because xml files are named by run number now, not by date.
# --------------------------------------------

import glob, re
from datetime import datetime, timezone
from xml.etree import ElementTree

from icecube import dataclasses, icetray

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import TypeVar
    from xml.etree.ElementTree import Element

    T = TypeVar("T", str, int, float)

class UpdateVEMCal(icetray.I3Module):
    '''
    This module locates the VEMCal file for a certain date (which is the big bunch of ugly work down here),
    And replaces the VEMCal values in the calibration frame.
    It puts the (wrong) values present in the L2 GCD in separate frame objects, since we need them for the correction.
    '''

    def __init__(self, ctx):
        super().__init__(ctx)
        self.AddParameter('Runnumber', 'Number of the run to be processed')
        self.AddParameter('XMLDir', 'Manual listing of the directory containing XML file (not necessary in Madison)')
        self.AddOutBox('OutBox')

    def Configure(self):
        self.runnumber = self.GetParameter('Runnumber')

        self.vemcal_l3  = dataclasses.I3VEMCalibrationMap()

        ## Did the user specify a directory by hand?
        xmlfilename = self.GetParameter('XMLDir')
        if (xmlfilename is None):
            icetray.logging.log_info('Using the default XML-file directory.')
            self.vemcal_files=glob.glob('/data/ana/CosmicRay/IceTop_OfflineCalib/vemcal_forpass3_ic86-2011-17/inject_IceTopVEMCalFit_*.xml')
        else:
            icetray.logging.log_info('Using user-defined XML-file directory: %s'%xmlfilename)
            self.vemcal_files=glob.glob(xmlfilename+'/inject_IceTopVEMCalFit_*.xml')

        if (len(self.vemcal_files)==0):
            icetray.logging.log_fatal('Your VEMcal XML-file directory seems to be empty! Use --VEMCal-dir option if not running in Madison.')
        self.vemcal_files.sort()

    def Calibration(self, frame):

        cal_l2 = frame['I3Calibration']
        cal = dataclasses.I3Calibration(cal_l2)
        self._find_VEMCal(cal.vem_cal.keys())
        # Replace:
        for key in cal.vem_cal.keys():
            cal.vem_cal[key] = self.vemcal_l3[key]
        frame.Delete('I3Calibration')
        frame.Put('I3Calibration', cal)
        self.PushFrame(frame)

    @classmethod
    def _value(cls, element: "ElementTree.Element", name: str, subtype: "type[T]") -> "T":
        child = element.find(name)
        if child is not None:
            if child.text is not None:
                return subtype(child.text)
            raise TypeError(f"element {child} contains no text")
        raise KeyError(name)

    def _find_VEMCal(self, l2_domkeys):
        # First find the correct vemcal file for that run.
        vc_file = self._find_VEMCal_file()

        tree=ElementTree.parse(vc_file)
        root=tree.getroot()
        for dom in root.findall('DOM'):
            string                 = self._value(dom, 'StringId', int)
            om                     = self._value(dom, 'TubeId', int)
            key                    = icetray.OMKey(string, om)
            if key not in self.vemcal_l3.keys():
                vemcal                 = dataclasses.I3VEMCalibration()
                vemcal.pe_per_vem      = round(self._value(dom, 'pePerVEM', float), 4)
                vemcal.mu_peak_width   = round(self._value(dom, 'muPeakWidth', float), 4)
                vemcal.hglg_cross_over = round(self._value(dom, 'hglgCrossOver', float), 2)
                vemcal.corr_factor     = self._value(dom, 'corrFactor', float)
                self.vemcal_l3[key]    = vemcal                
        
    # Find appropriate VEMCal XML file.
    def _find_VEMCal_file(self):
        found=False
        for i in range(0, len(self.vemcal_files)):
            vemcal_file=self.vemcal_files[i]
            vemcal_runs=re.search("inject_IceTopVEMCalFit_([0-9-]+)",vemcal_file).group(1).split("-")
            if (int(vemcal_runs[0])<=self.runnumber) and (self.runnumber<=int(vemcal_runs[1])):
                found=True
                break
        
        if found:
            icetray.logging.log_info('Using VEMCAL: {0}'.format(vemcal_file))
        else:
            icetray.logging.log_fatal('No VEMCal directory for this run seems to exist.')
        return vemcal_file

    # Validate that we have the right VEMCal file
    def _validate(self,vc_file):
        tree = ElementTree.parse(vc_file)
        root = tree.getroot()
        inFile=False
        first_run = self._value(root, 'FirstRun', int)
        last_run  = self._value(root, 'LastRun', int)
        if self.runnumber < first_run or self.runnumber > last_run:
            icetray.logging.log_info('Runnumber to process ({0}) out of valid range from {1} to {2}. Possibly try again.'.format(self.runnumber, first_run, last_run))
        else:
            inFile=True

        return inFile
