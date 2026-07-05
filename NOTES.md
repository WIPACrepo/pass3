# On GCD Files

We used the GCD files from Level2Pass2a as input for Pass3. The major changes are that:

* SPEs are fit with 1 Gaussian + 2 exponentials
* Changes in the wavedeform algorithm to avoid extra pulse splitting
* mean atwd/fadc charge = 1 for all DOMs
* fadc gain changes

DOM 21, 3 had a failed FADC charge fit for IC86-2014


what happened in 2014_5_new_gcd.slurm.e.716155?

Traceback (most recent call last):
  File "/opt/pass3/scripts/icetray/step1/run_step1.py", line 593, in <module>
    inputs = prepare_inputs(args.outdir,
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/pass3/scripts/icetray/step1/run_step1.py", line 221, in prepare_inputs
    if (runnum in grl) and (normalized_member not in bad_file_members):
        ^^^^^^
UnboundLocalError: cannot access local variable 'runnum' where it is not associated with a value
srun: error: i618-052: task 17: Exited with exit code 1

there is a weird file in the bundle: DebugData_PFRaw124751_001.tar.gz




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

No GCD found for 128826
No GCD found for 131264
No GCD found for 131243
No GCD found for 133284
No GCD found for 133408
No GCD found for 129212
No GCD found for 128826
No GCD file found for run 125068
No GCD file found for run 124703

extracted files:
Broken files:
['PFRaw_PhysicsFiltering_Run00127130_Subrun00000000_00000060.tar.gz', 'PFRaw_PhysicsFiltering_Run00127129_Subrun00000000_00000080.tar.gz']
 ['PFRaw_PhysicsFiltering_Run00133046_Subrun00000000_00000113.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2019/0909/a6a39f10e5a311ea84e646aae246590a.zip
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00127130_Subrun00000000_00000060.tar.gz', 'PFRaw_PhysicsFiltering_Run00127129_Subrun00000000_00000080.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/1118/2bec33fd-2973-4668-91fc-aa34784d5ec3.zip
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00126158_Subrun00000000_00000114.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/0316/a04c459e-8d69-4e2f-beea-06242e765f9c.zip
['PFRaw_PhysicsFiltering_Run00127000_Subrun00000000_00000164.tar.gz'] - PnF crash
key_32609784_PFRaw_PhysicsFiltering_Run00128987_Subrun00000000_00000060.tar.gz - gps lost power


Exception: Did not finish ['key_803125_PFRaw_PhysicsFiltering_Run00125553_Subrun00000000_00000113.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/1112/b555e0e0-47d8-4d62-8418-b3595add3f55.zip

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125737_Subrun00000000_00000038.tar.gz', 'PFRaw_PhysicsFiltering_Run00125737_Subrun00000000_00000126.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/1216/195c0087-c6a4-4624-80f3-3735cffb5569.zip
srun: error: i615-082: task 15: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125756_Subrun00000000_00000223.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/1221/739a140e-0577-40ae-aedf-a65c9b7c6bfe.zip
srun: error: i617-014: task 20: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125760_Subrun00000000_00000181.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/1222/fa0d1e0e-36d4-4ef3-831f-cae8edd8a253.zip
srun: error: i617-044: task 21: Exited with exit code 1

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00127356_Subrun00000000_00000150.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2016/0104/e2b7f4bf-7194-4dfd-93e1-29ee1c47807f.zip

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00123701_Subrun00000000_00000117.tar.gz', 'PFRaw_PhysicsFiltering_Run00123701_Subrun00000000_00000116.tar.gz', 'PFRaw_PhysicsFiltering_Run00123701_Subrun00000000_00000120.tar.gz', 'PFRaw_PhysicsFiltering_Run00123701_Subrun00000000_00000122.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0106/f9356653-1f8a-4037-b887-d847a088709e.zip

broken not yet extracted:

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125216_Subrun00000000_00000131.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0822/2666ae38-a76e-42ed-a200-70d3a08744a5.zip
srun: error: i617-082: task 21: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125175_Subrun00000000_00000063.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0814/e68ea53c-1fba-450a-988c-2ab25da14dae.zip
srun: error: i615-111: task 13: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125237_Subrun00000000_00000001.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0829/a078bc69-5c87-452d-93c6-6f6048e32308.zip
srun: error: i618-082: task 29: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125240_Subrun00000000_00000126.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0830/f3945da7-532c-499a-8a76-f04d8dddcc56.zip
srun: error: i618-083: task 30: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125174_Subrun00000000_00000091.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0813/5442602d-51c6-4acb-9d3f-f404e30f78c1.zip
srun: error: i615-104: task 12: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125214_Subrun00000000_00000194.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0822/2f46939f-d329-4f2a-9a17-18edcef57d50.zip
srun: error: i617-083: task 22: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125145_Subrun00000000_00000051.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0807/d7859abd-bd2e-4f8b-bde0-50781eb88b02.zip
srun: error: i615-053: task 6: Exited with exit code 1

['key_31596557_PFRaw_PhysicsFiltering_Run00128175_Subrun00000000_00000065.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2016/0626/67e99bd0-a307-4af4-97fe-6737bbb670c1.zip


Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00124960_Subrun00000000_00000039.tar.gz', 'PFRaw_PhysicsFiltering_Run00124962_Subrun00000000_00000004.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0625/2f1d7f86-c60c-4371-a048-5afb1223a717.zip

What happened here:

Exception: Unexpected infile name format: .PFRaw_PhysicsFiltering_Run00125781_Subrun00000000_00000192.tar.gz.7OMtlu

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00127230_Subrun00000000_00000039.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/1211/713acf8a-a617-4b9b-9736-ff0dd4e75b27.zip
srun: error: i614-083: task 10: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00127238_Subrun00000000_00000092.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/1213/f1a7c422-9395-49ca-be2c-fa66a3ac2c7c.zip
srun: error: i614-091: task 12: Exited with exit code 1

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00126627_Subrun00000000_00000199.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/0717/84fe1f3a-d458-4d12-93f7-2b8d3e227929.zip
srun: error: i617-081: task 16: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00126555_Subrun00000000_00000125.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/0702/37516e38-dace-42a2-9c62-0f2803dde0b0.zip
srun: error: i615-001: task 1: Exited with exit code 1

Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125792_Subrun00000000_00000078.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/0101/66d504b7-5b77-4425-8a1f-0ba1e9379700.zip
srun: error: i614-144: task 0: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125797_Subrun00000000_00000160.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/0103/e19d5461-de7a-4fb3-9ccd-9004e0908f99.zip
srun: error: i615-043: task 2: Exited with exit code 1
Exception: Did not finish ['PFRaw_PhysicsFiltering_Run00125801_Subrun00000000_00000080.tar.gz', 'PFRaw_PhysicsFiltering_Run00125802_Subrun00000000_00000106.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2015/0104/a7871eaf-9e76-4273-b49a-df921535d8b9.zip
srun: error: i615-044: task 3: Exited with exit code 1

['PFRaw_PhysicsFiltering_Run00124982_Subrun00000000_00000092.tar.gz'] from bundle /scratch/04799/tg840985/tmp.v2/2014/0701/606206f6-16d3-453d-8404-7ba7dc687d82.zip

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



PFRaw_PhysicsTrig_PhysicsFiltering_Run00121106_Subrun00000000_00000000.tar.gz /scratch/04799/tg840985/tmp.v2/2012/1126/bf57331c-4ca9-4555-8a3e-35f4044b062b.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119325_Subrun00000000_00000117.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0106/a31e6eed-6670-4c77-884b-116cdc8ebdd0.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119325_Subrun00000000_00000113.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0106/a31e6eed-6670-4c77-884b-116cdc8ebdd0.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119325_Subrun00000000_00000115.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0106/a31e6eed-6670-4c77-884b-116cdc8ebdd0.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119340_Subrun00000000_00000062.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0107/b216002d-df23-4e03-8ca5-914dc8b176ea.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119340_Subrun00000000_00000064.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119340_Subrun00000000_00000060.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119340_Subrun00000000_00000073.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119344_Subrun00000000_00000174.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119347_Subrun00000000_00000162.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119347_Subrun00000000_00000179.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119347_Subrun00000000_00000180.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119347_Subrun00000000_00000182.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0109/f0432a94-4a39-4024-87b3-4ed0f0bbce63.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119343_Subrun00000000_00000029.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119344_Subrun00000000_00000035.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119341_Subrun00000000_00000034.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119343_Subrun00000000_00000168.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119344_Subrun00000000_00000051.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119343_Subrun00000000_00000030.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119344_Subrun00000000_00000033.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119341_Subrun00000000_00000036.tar.gz /scratch/04799/tg840985/tmp.v2/2012/0108/b3c42840-8a01-4b26-b3d1-216b1dea1dab.zip
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000206.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000194.tar.gz 
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000014.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000211.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000178.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000219.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000198.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000016.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000183.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000204.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000215.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000009.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000021.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000001.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000181.tar.gz
PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000222.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000005.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000200.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000202.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000010.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000208.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000012.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000220.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000003.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000224.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000007.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000196.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000189.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000187.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119426_Subrun00000000_00000018.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000190.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000176.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000213.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000217.tar.gz', 'PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000192.tar.gz', PFRaw_PhysicsTrig_PhysicsFiltering_Run00119425_Subrun00000000_00000185.tar.gz'



YEAR=2020; MONTH=2; python3 /home1/04799/tg840985/pass3/scripts/submit/step1/submit_stampede3.py --container /scratch/04799/tg840985/pass3_cvmfs_v4.4.2_icetray_v1.17.0_arm.sif --submitfile /home1/04799/tg840985/pass3/submit_files/${YEAR}/${YEAR}_${MONTH}_new_gcd.slurm --checksum-file /home1/04799/tg840985/pass3/data/checksums.sha512sum --year ${YEAR} --month ${MONTH} --gcddir /work/04799/tg840985/stampede3/GCD.v2/ --multiprogfile /home1/04799/tg840985/pass3/submit_files/${YEAR}/${YEAR}_${MONTH}_new_gcd.multiprog --slurmqueue gg --numcores 100 --allocation PHY20012 --grl /home1/04799/tg840985/pass3/data/grl.pass3 --badfiles /home1/04799/tg840985/pass3/data/known_bad_files --cpuarch aarch64 --bundlesready --bundledir $SCRATCH/tmp.v2/ --outdir $SCRATCH/testout.v2/${YEAR} --duplicate-skip-dir /home1/04799/tg840985/pass3/submit_files/${YEAR}/skip/