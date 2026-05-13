# On GCD Files

We used the GCD files from Level2Pass2a as input for Pass3. The major changes are that:

* SPEs are fit with 1 Gaussian + 2 exponentials
* Changes in the wavedeform algorithm to avoid extra pulse splitting
* mean atwd/fadc charge = 1 for all DOMs
* fadc gain changes

DOM 21, 3 had a failed FADC charge fit for IC86-2014

Runs with GCD issues:

Skipping run 121106: No Pass2 GCD found for run 121106. Searched:
  /data/exp/IceCube/2012/filtered/level2pass2a/1126/Run00121106
Skipping run 124311: No Pass2 GCD found for run 124311. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0227/Run00124311
Skipping run 124312: No Pass2 GCD found for run 124312. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0227/Run00124312
  /data/exp/IceCube/2014/filtered/level2pass2a/0228/Run00124312
Skipping run 124315: No Pass2 GCD found for run 124315. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0228/Run00124315
Skipping run 124567: No Pass2 GCD found for run 124567. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0411/Run00124567
Skipping run 125630: No Pass2 GCD found for run 125630. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1126/Run00125630
Skipping run 125631: No Pass2 GCD found for run 125631. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1126/Run00125631
Skipping run 125634: No Pass2 GCD found for run 125634. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1126/Run00125634
  /data/exp/IceCube/2014/filtered/level2pass2a/1127/Run00125634
Skipping run 125654: No Pass2 GCD found for run 125654. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1129/Run00125654
Skipping run 125656: No Pass2 GCD found for run 125656. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1129/Run00125656
  /data/exp/IceCube/2014/filtered/level2pass2a/1130/Run00125656
Skipping run 125657: No Pass2 GCD found for run 125657. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1130/Run00125657
Skipping run 125658: No Pass2 GCD found for run 125658. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1130/Run00125658
Skipping run 128007: No Pass2 GCD found for run 128007. Searched:
  /data/exp/IceCube/2016/filtered/level2pass2a/0606/Run00128007
Skipping run 129212: No Pass3 GCD found for run 129212 in /data/ana/IceCube/2017/filtered/debug.v2/GCD
Skipping run 131264: No Pass2 GCD found for run 131264. Searched:
  /data/exp/IceCube/2018/filtered/level2/0710/Run00131264
Skipping run 133284: No Pass2 GCD found for run 133284. Searched:
  /data/exp/IceCube/2019/filtered/level2/1114/Run00133284
Skipping run 133408: No Pass2 GCD found for run 133408. Searched:
  /data/exp/IceCube/2019/filtered/level2/1208/Run00133408

  Skipping run 124304: No Pass2 GCD found for run 124304. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0225/Run00124304
Skipping run 124305: No Pass2 GCD found for run 124305. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0225/Run00124305
Skipping run 124306: No Pass2 GCD found for run 124306. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0225/Run00124306
  /data/exp/IceCube/2014/filtered/level2pass2a/0226/Run00124306
Skipping run 124310: No Pass2 GCD found for run 124310. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0227/Run00124310
Skipping run 124313: No Pass2 GCD found for run 124313. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0228/Run00124313
Skipping run 124314: No Pass2 GCD found for run 124314. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0228/Run00124314
Skipping run 124703: No Pass2 GCD found for run 124703. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0506/Run00124703
  /data/exp/IceCube/2014/filtered/level2pass2a/0507/Run00124703
Skipping run 125068: No Pass2 GCD found for run 125068. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/0720/Run00125068
Skipping run 125629: No Pass2 GCD found for run 125629. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1126/Run00125629
Skipping run 125632: No Pass2 GCD found for run 125632. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1126/Run00125632
Skipping run 125633: No Pass2 GCD found for run 125633. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1126/Run00125633
Skipping run 125635: No Pass2 GCD found for run 125635. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1127/Run00125635
Skipping run 125648: No Pass2 GCD found for run 125648. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1129/Run00125648
Skipping run 125649: No Pass2 GCD found for run 125649. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1129/Run00125649
Skipping run 125659: No Pass2 GCD found for run 125659. Searched:
  /data/exp/IceCube/2014/filtered/level2pass2a/1130/Run00125659
  /data/exp/IceCube/2014/filtered/level2pass2a/1201/Run00125659
Skipping run 128826: No Pass2 GCD found for run 128826. Searched:
  /data/exp/IceCube/2016/filtered/level2pass2a/1203/Run00128826
Skipping run 131243: No Pass2 GCD found for run 131243. Searched:
  /data/exp/IceCube/2018/filtered/level2/0705/Run00131243
Skipping run 131263: No Pass2 GCD found for run 131263. Searched:
  /data/exp/IceCube/2018/filtered/level2/0710/Run00131263


  Exception: Did not finish ['key_32609784_PFRaw_PhysicsFiltering_Run00128987_Subrun00000000_00000060.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2016/1231/f85ec33f-e09e-476d-9fd5-55fac27d0fd2.zip

FileNotFoundError: No GCD found for 128826
FileNotFoundError: No GCD found for 131264
FileNotFoundError: No GCD found for 131243
FileNotFoundError: No GCD found for 133284
FileNotFoundError: No GCD found for 133408

FileNotFoundError: No GCD found for 129212
FileNotFoundError: No GCD found for 128826

extracted files:
Broken files:
['PFRaw_PhysicsFiltering_Run00127130_Subrun00000000_00000060.tar.gz', 'PFRaw_PhysicsFiltering_Run00127129_Subrun00000000_00000080.tar.gz']


broken not yet extracted:

['PFRaw_PhysicsFiltering_Run00127000_Subrun00000000_00000164.tar.gz'] - PnF crash

key_32609784_PFRaw_PhysicsFiltering_Run00128987_Subrun00000000_00000060.tar.gz - gps lost power


For 

PFRaw_PhysicsFiltering_Run00136445_Subrun00000000_00000111.tar.gz
PFRaw_PhysicsFiltering_Run00136445_Subrun00000000_00000112.tar.gz
PFRaw_PhysicsFiltering_Run00136445_Subrun00000000_00000113.tar.gz

there was an PnF crash: https://icecube-spno.slack.com/archives/C02M5UQPP/p1647609551801449

the pass3_reprocess_PFRaw.py script barfs at some eventheaders inside an If= statement

briedel@cobalt-15 14:08:40  /data/exp/IceCube/2022/filtered/pass3/step1/0318/broken_pfraw
 12 $ python3 /home/briedel/code/pass3/main/scripts/icetray/step1/pass3_reprocess_PFRaw.py -i ./PFRaw_PhysicsFiltering_Run00136445_Subrun00000000_00000111.tar.gz -o Pass3_Step1_PhysicsFiltering_Run00136445_Subrun00000000_00000111.tar.gz -g /data/ana/IceCube/2022/filtered/debug.v2/GCD/OnlinePass3_IC86.2021_data_Run00136445_82_663_GCD.i3.zst
Warning in <UnknownClass::SetDisplay>: DISPLAY not set, setting it to 144.92.100.50:0.0
pass3_reprocess_PFRaw.py:79 WARNING: GCD path and file: /data/ana/IceCube/2022/filtered/debug.v2/GCD OnlinePass3_IC86.2021_data_Run00136445_82_663_GCD.i3.zst
i3tray.py:46 DEBUG: Adding Anonymous Module of type '<function Fix_LeapSecond at 0x7fd3793418a0>' with name 'Fix_LeapSecond_0000'
base_processing.py:90 WARNING: Disabled: Wavedeform bypass condition.
i3tray.py:46 DEBUG: Adding Anonymous Segment of type 'online_filters' with name 'online_filters_0000'
i3tray.py:46 DEBUG: Adding Anonymous Module of type '<function Cascade_filter.<locals>.CascadeFilterSelect at 0x7fd36b89c040>' with name 'CascadeFilterSelect_0000'
i3tray.py:46 DEBUG: Adding Anonymous Module of type '<function L2Reco.<locals>.FindBestTrack at 0x7fd36b89c7c0>' with name 'FindBestTrack_0000'
i3tray.py:46 DEBUG: Adding Anonymous Module of type '<function alert_event_extractor.<locals>.unhide_QEventHeader at 0x7fd36b8b1300>' with name 'unhide_QEventHeader_0000'
i3tray.py:46 DEBUG: Adding Anonymous Module of type '<function alert_event_extractor.<locals>.cleaup_QEventHeader at 0x7fd36b8b1620>' with name 'cleaup_QEventHeader_0000'
Initializing cpandel parameterization ... done
I3Module.cxx:141  ERROR : Base_DOMCleaning_baddomclean: Exception thrown
Traceback (most recent call last):
  File "/home/briedel/code/pass3/main/scripts/icetray/step1/pass3_reprocess_PFRaw.py", line 269, in <module>
    tray.Execute()
  File "/cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/RHEL_9_x86_64_v2/metaprojects/icetray/v1.17.0/lib/python3.12/site-packages/icecube/icetray/i3tray.py", line 223, in Execute
    super().Execute()
  File "/cvmfs/icecube.opensciencegrid.org/py3-v4.4.2/RHEL_9_x86_64_v2/metaprojects/icetray/v1.17.0/lib/python3.12/site-packages/icecube/online_filterscripts/base_segments/onlinecalibration.py", line 36, in <lambda>
    If=lambda frame: frame["I3EventHeader"].run_id < 140808)
                     ~~~~~^^^^^^^^^^^^^^^^^
KeyError: 'I3EventHeader'

One can open the files and the "broken" eventheaders in dataio-shovel and with a separate icetray script with the same results. this needs further investigation