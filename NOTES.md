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