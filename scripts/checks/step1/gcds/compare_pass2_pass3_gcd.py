#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/icetray-start
#METAPROJECT /data/user/briedel/pass3/icetray/v1.17.0/build/
"""
Changes we expect to see between Pass2 and Pass3 GCDs:
- Pass3 GCDs should have mean_atwd_charge_correction and mean_fadc_charge_correction set to 1.0 for all DOMs, and should not be NaN.
- Pass3 GCDs should have the FADC gain correction applied, which means that the FADC gain value in the GCD should be different from the old FADC gain value by the correction factor, and the difference should not be NaN.
- There should be no change to the relative DOM efficiency values, and they should not be NaN.
- Assumed SPE shapes between Pass2a and Pass3 GCD should be different
- Assumed SPE shapes between Pass2b and Pass3 GCD should be the same, so we can use the SPE shape information to determine which Pass2 GCD we are comparing to.
"""
import argparse
import math
from pathlib import Path
import warnings
import json


EXPECTED_CHANGED_ATTRIBUTES = [
    "mean_atwd_charge_correction",
    "mean_fadc_charge_correction",
    "fadc_gain",
    "relative_dom_eff",
    "combined_spe_charge_distribution",
]

SKIPPED_ATTRIBUTES = [
    "atwd_beacon_baseline",
    "atwd_bin_calib_slope",
    "atwd_delta_t",
    "atwd_freq_fit",
    "atwd_gain",
    "atwd_pulse_template",
    "fadc_pulse_template",
    "discriminator_pulse_template",
]

SUMMARY_EXPECTED_CHANGED_ATTRIBUTES = {
    "mean_atwd_charge_correction",
    "mean_fadc_charge_correction",
    "fadc_gain",
}


def make_diffs() -> dict:
    return {
        "cal": {
            "changed": {},
            "expected": {},
            "skipped": SKIPPED_ATTRIBUTES[:],
            "NaNs": {},
            "valid_fadc_different": {},
            "valid_atwd_different": {},
            "charge_dist_different": {},
            "attributed_different": False,
        },
        "geo": {},
        "det_status": {},
    }


def init_dom_cal_diff(cal_diffs: dict, dom_key: str) -> None:
    cal_diffs["expected"][dom_key] = []
    cal_diffs["changed"][dom_key] = []
    cal_diffs["NaNs"][dom_key] = []
    cal_diffs["valid_fadc_different"][dom_key] = []
    cal_diffs["valid_atwd_different"][dom_key] = []
    cal_diffs["charge_dist_different"][dom_key] = False


def make_summary_diffs() -> dict:
    return {
        "cal": {
            "changed": {},
            "expected": {},
            "NaNs": {},
            "charge_dist_different": [],
        }
    }




def read_gcd_file(filepath: Path):
    """Reads a GCD file and returns the calibration, geometry, and detector status information."""
    from icecube import dataio
    from icecube import icetray

    cal = None
    geo = None
    det_status = None

    with dataio.I3File(str(filepath), "r") as f:
         while f.more():
            frame = f.pop_frame()
            if frame.Stop == icetray.I3Frame.Calibration:
                cal = frame["I3Calibration"]
            elif frame.Stop == icetray.I3Frame.Geometry:
                geo = frame["I3Geometry"]
            elif frame.Stop == icetray.I3Frame.DetectorStatus:
                det_status = frame["I3DetectorStatus"]

    if cal is None or geo is None or det_status is None:
        raise ValueError(f"Missing calibration, geometry, or detector status frame in {filepath}")

    return cal, geo, det_status

def get_dom_cal_attributes(dom_cal):
    """need to use dir() to get the list of attributes for the DOMCal objects, since they are not dataclasses and do not have __dict__ attribute, so we cannot just compare the __dict__ of the two DOMCal objects to see which attributes differ, we need to get the list of attributes using dir() and then compare the values of those attributes between the two DOMCal objects."""
    return [a for a in dir(dom_cal) if not a.startswith('_')]


def values_are_both_nan(value1, value2) -> bool:
    try:
        return math.isnan(value1) and math.isnan(value2)
    except TypeError:
        return False


def handle_expected_calibration_difference(dom, dom_key: str, attribute: str,
                                           value_base, value_compare,
                                           cal_base_dom, cal_compare_dom,
                                           diffs: dict) -> None:
    print(
        f"EXPECTED: Attribute {attribute} differs for DOM {dom} between baseline and comparison GCDs, "
        f"but we expect it to differ. baseline value: {value_base}, comparison value: {value_compare}"
    )

    if attribute == "combined_spe_charge_distribution":
        if value_base.pdfs == value_compare.pdfs:
            print(
                f"SPE charge distribution is the same for DOM {dom} between baseline and comparison GCDs, "
                f"but we expected it to differ. baseline value: {cal_base_dom.combined_spe_charge_distribution}, "
                f"comparison value: {cal_compare_dom.combined_spe_charge_distribution}"
            )
        else:
            print(
                f"SPE charge distribution differs for DOM {dom} between baseline and comparison GCDs. "
                f"baseline value: {cal_base_dom.combined_spe_charge_distribution}, "
                f"comparison value: {cal_compare_dom.combined_spe_charge_distribution}"
            )
            diffs["cal"]["charge_dist_different"][dom_key] = True
        return

    if attribute == "relative_dom_eff" and math.isclose(value_base, value_compare, rel_tol=1e-6, abs_tol=1e-9):
        return

    diffs["cal"]["expected"][dom_key].append((attribute, value_base, value_compare))


def handle_nan_calibration_difference(dom, dom_key: str, attribute: str,
                                      value_base, value_compare, diffs: dict) -> bool:
    if not values_are_both_nan(value_base, value_compare):
        return False

    print(
        f"Attribute {attribute} is NaN for DOM {dom} in both baseline and comparison GCDs, "
        f"but we expected it to be different between the two GCDs."
    )
    diffs["cal"]["NaNs"][dom_key].append((attribute, value_base, value_compare))
    return True


def handle_validity_flag_difference(dom, dom_key: str, attribute: str,
                                    value_base, value_compare,
                                    cal_base_dom, cal_compare_dom,
                                    diffs: dict) -> bool:
    if attribute == "is_mean_fadc_charge_correction_valid":
        print(
            f"FADC charge correction differs for DOM {dom} between baseline and comparison GCDs. "
            f"baseline value: {cal_base_dom.mean_fadc_charge_correction}, "
            f"comparison value: {cal_compare_dom.mean_fadc_charge_correction}"
        )
        diffs["cal"]["valid_fadc_different"][dom_key].append((attribute, value_base, value_compare))
        return True

    if attribute == "is_mean_atwd_charge_correction_valid":
        print(
            f"ATWD charge correction differs for DOM {dom} between baseline and comparison GCDs. "
            f"baseline value: {cal_base_dom.mean_atwd_charge_correction}, "
            f"comparison value: {cal_compare_dom.mean_atwd_charge_correction}"
        )
        diffs["cal"]["valid_atwd_different"][dom_key].append((attribute, value_base, value_compare))
        return True

    return False


def handle_unexpected_calibration_difference(dom, dom_key: str, attribute: str,
                                             value_base, value_compare,
                                             diffs: dict) -> None:
    diffs["cal"]["changed"][dom_key].append((attribute, value_base, value_compare))
    warnings.warn(
        f"{attribute} differs between baseline and comparison GCDs, but we do not expect it to differ. "
        f"baseline value: {value_base}, comparison value: {value_compare}",
        UserWarning,
    )

def compare_pass3_calibrations(cal_base, cal_compare, diffs: dict):
    for k, c1 in cal_base.dom_cal.items():
        if k.om > 60:
            # Skip IceTop DOMs
            continue
        if k.string == 0:
            # skip iceact
            continue
        dom_key = str(k)
        if dom_key not in diffs["cal"]["expected"]:
            init_dom_cal_diff(diffs["cal"], dom_key)
        c2 = cal_compare.dom_cal[k]
        attributes_base = get_dom_cal_attributes(c1)
        attributes_compare = get_dom_cal_attributes(c2)
        if set(attributes_base) != set(attributes_compare):
            print(f"Attributes differ for DOM {k} between baseline and comparison GCDs. Baseline attributes: {attributes_base}, Comparison attributes: {attributes_compare}")
            diffs["cal"]["attributed_different"] = True
        for a, v in [(a, getattr(c1, a)) for a in attributes_base]:
            # print(f"Comparing attribute {a} for DOM {k}")
            # These are evil properties!
            if a in diffs["cal"]["skipped"]:
                print(f"Skipping comparison of attribute {a} for DOM {k} since it is expected to differ between GCDs.")
                # diffs["cal"]["skipped"][str(k)].append(a)
                continue
            v2 = getattr(c2, a)
            if v != v2:
                if a in EXPECTED_CHANGED_ATTRIBUTES:
                    handle_expected_calibration_difference(k, dom_key, a, v, v2, c1, c2, diffs)
                    continue
                if handle_nan_calibration_difference(k, dom_key, a, v, v2, diffs):
                    continue
                if handle_validity_flag_difference(k, dom_key, a, v, v2, c1, c2, diffs):
                    continue
                handle_unexpected_calibration_difference(k, dom_key, a, v, v2, diffs)

def compare_calibrations_attributes(cal_base, cal_compare):
    for key in cal_base.dom_cal.keys():
        if key.string == 0:
            continue
        item2 = cal_compare.dom_cal[key]
        item1 = cal_base.dom_cal[key]
        attributes_base = get_dom_cal_attributes(item1)
        attributes_compare = get_dom_cal_attributes(item2)
        if set(attributes_base) != set(attributes_compare):
            print(f"Attributes differ for DOM {key} between baseline and comparison GCDs. Baseline attributes: {attributes_base}, Comparison attributes: {attributes_compare}")
        for a, v in [(a, getattr(item1, a)) for a in attributes_base]:
            print(f"Comparing attribute {a} for DOM {key}")
            v2 = getattr(item2, a)
            # Looking at the SPE PDFs specifically to see if they differ between Pass2a and Pass2b GCDs, since Pass3 GCDs are based on Pass2b GCDs, so we expect them to be the same between Pass2b and Pass3 GCDs but different between Pass2a and Pass3 GCDs.
            if a == "combined_spe_charge_distribution":
                if v.pdfs == v2.pdfs:
                    print(f"SPE charge distribution PDFs are the same for DOM {key} between baseline and comparison GCDs.")
                else:
                    print(f"SPE charge distribution PDFs differ for DOM {key} between baseline and comparison GCDs.")
            if v != v2:
                print(f"DOM {key}, variable {a} differs between baseline and comparison GCDs. Baseline value: {v}, Comparison value: {v2}")


def summarize_distinct_attribute_sets(summary_bucket: dict,
                                      dom_attribute_diffs: dict,
                                      ignored_attributes=None) -> None:
    first_reported_attributes = None

    for dom_key, attribute_diffs in dom_attribute_diffs.items():
        attributes = {attribute_name for attribute_name, *_ in attribute_diffs}
        if not attributes:
            continue
        if ignored_attributes is not None and attributes == ignored_attributes:
            continue
        if first_reported_attributes is None:
            first_reported_attributes = attributes
            summary_bucket[dom_key] = sorted(attributes)
            continue
        if attributes != first_reported_attributes:
            summary_bucket[dom_key] = sorted(attributes)

def summary_diffs_cal(diffs: dict) -> dict:
    summary = make_summary_diffs()

    for dom_key, changed_attributes in diffs["cal"]["changed"].items():
        if changed_attributes:
            summary["cal"]["changed"][dom_key] = changed_attributes

    summarize_distinct_attribute_sets(
        summary["cal"]["expected"],
        diffs["cal"]["expected"],
        ignored_attributes=SUMMARY_EXPECTED_CHANGED_ATTRIBUTES,
    )
    summarize_distinct_attribute_sets(summary["cal"]["NaNs"], diffs["cal"]["NaNs"])

    summary["cal"]["charge_dist_different"] = [
        dom_key
        for dom_key, charge_dist_changed in diffs["cal"]["charge_dist_different"].items()
        if charge_dist_changed
    ]

    return summary


def compare_gcds(gcd_base: Path, 
                 gcd_compare: Path,
                 comparison_json: Path,
                 summary_json: Path) -> None:
    """Compares the Pass2 and Pass3 GCD files for expected changes."""

    print(f"Comparing GCD file {gcd_base} to GCD file {gcd_compare}")

    cal_base, geo_base, det_status_base = read_gcd_file(gcd_base)
    cal_comp, geo_comp, det_status_comp = read_gcd_file(gcd_compare)

    diffs = make_diffs()

    if cal_base == cal_comp:
        raise Exception(f"Calibration information is identical between baseline {gcd_base} and comparison {gcd_compare} GCDs, but we expect it to differ due to the charge corrections and FADC gain correction.")
    else:
        compare_calibrations_attributes(cal_base, cal_comp)
        compare_pass3_calibrations(cal_base, cal_comp, diffs)

    if (det_status_base != det_status_comp):
        # Detector status information should not change between Pass2 and Pass3 GCDs, so if they differ, we raise an error.
        raise Exception(f"Detector status information differs between baseline {gcd_base} and comparison {gcd_compare} GCDs.")
    
    # if (geo_base != geo_comp):
    #     # Geometry information should not change between Pass2 and Pass3 GCDs, so if they differ, we raise an error.
    #     raise Exception(f"Geometry information differs between baseline {gcd_base} and comparison {gcd_compare} GCDs.")

    with open(comparison_json, "w") as f:
        json.dump(diffs, f, indent=4)
    with open(summary_json, "w") as f:
        json.dump(summary_diffs_cal(diffs), f, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pass2-gcd", 
                        help="Pass2 GCD file", 
                        type=Path, 
                        required=True)
    parser.add_argument("--pass3-gcd", 
                        help="Pass3 GCD file", 
                        type=Path, 
                        required=True)
    parser.add_argument("--output-diffs-json", 
                        help="Output JSON file to write the detailed differences between the GCDs",
                        type=Path, 
                        required=True)
    parser.add_argument("--output-summary-json", 
                        help="Output JSON file to write the summary of differences between the GCDs", 
                        type=Path, 
                        required=True)
    args = parser.parse_args()

    print(f"Comparing  GCD file {args.pass2_gcd} as baseline to GCD file {args.pass3_gcd}")

    compare_gcds(args.pass2_gcd, 
                 args.pass3_gcd,
                 args.output_diffs_json,
                 args.output_summary_json)