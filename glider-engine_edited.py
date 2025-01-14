#!/usr/bin/python

import sys
import os
import numpy as np
import math as ma
from datetime import timedelta
from datetime import datetime
from geopy import distance
from itertools import islice
import rasterio as rio
import glob
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Threshold values for MOP sensor
thresholds = [300, 400, 500, 600, 700]

# We use one global lock to protect CSV writing across threads
csv_lock = threading.Lock()

def c_time(file, csv_writers, dband1, demdata):
    """
    Process one IGC file, detect flights, engine runs, etc.
    Writes results to CSV using a shared `csv_writers` dict.
    We only use the short name (basename) for the "File" column in the CSV.
    """
    import os

    # Use short filename for CSV output:
    igcfn = os.path.basename(file)  # e.g. "4536-9239003146.igc"

    try:
        # Open file for reading
        fn = open(file, encoding='utf-8', errors='ignore')
    except Exception as e:
        print(f"Could not open file {file}: {e}")
        return

    print(f"Processing file: {file}")  # Full path for debugging

    # ---------------------------
    # Original variable setup
    # ---------------------------
    atime = alat = alon = aN = aW = aF = apress = agnss = btime = blen = 0
    spd = start = st = spress = bcnt = stop = hpress = mpress = dpress = 0
    eng = rpm = enl = mop = engval = enlval = mopval = engflg = enlflg = 0
    fdate = 'Unknown'
    seng = []
    agleng = []
    msleng = []
    senl = []
    aglenl = []
    mslenl = []
    Iline = ''
    FMT = '%H%M%S'
    gid = 'Unknown'
    gtype = 'Unknown'
    cid = 'Unknown'
    pname = 'P.Pilot'
    HAT = onc = maxaglalt = aglalt = ilen = 0
    shat = []
    mhat = []
    that = []
    ahat = []
    S2H = 3600
    M2F = 3.28084
    K2M = .621371
    xtime = '00:03:30'
    tas = gsp = sginit = demalt = dist = 0

    # Initialize sensor_info variable for engine runs
    sensor_info = ''

    # Initialize threshold-based data structures
    mop_data = {}
    for t in thresholds:
        mop_data[t] = {
            'mopflg': 0,
            'smop': [],
            'mslmop': [],
            'aglmop': [],
            'sensor_info': ''
        }

    try:
        # ---------------------------
        # Main logic from original script
        # ---------------------------
        for line in fn:
            line = line.strip().rstrip("\n").rstrip("\r")
            igc = ''.join(islice(line, 1))
            if igc == 'H':
                hstr = line.split(":")
                if hstr[0] == 'HFGTYGLIDERTYPE':
                    gtype = ''.join((hstr[1]).split())
                    continue
                hdate = ''.join(islice(line, 5))
                if hdate == 'HFDTE':
                    res = str(line).split(":")
                    if len(res) > 1:
                        res2 = res[1].split(",")
                        if len(res2) > 1:
                            fltdate = res2[0]
                        else:
                            fltdate = res[1]
                    else:
                        fltdate = (''.join(islice(line, 5, 12)))
                    fday = (''.join(islice(fltdate, 0, 2)))
                    fmon = (''.join(islice(fltdate, 2, 4)))
                    fyr = (''.join(islice(fltdate, 4, 6)))
                    fdate = (str(fmon) + '/' + str(fday) + '/20' + str(fyr))
                    if (str(fdate) == ''):
                        fdate = 'Unknown'
                    continue
            if igc == 'I':
                cnt = ''.join(islice(line, 1, 3))
                if ((not cnt.isdigit()) or (int(cnt) == 0)):
                    continue
                Iline = line
                j = 7
                k = len(line)
                sensor = ''
                for i in range(int(cnt)):
                    tag = ''.join(islice(line, j, j+3))
                    if tag == 'TAS':
                        tas = ''.join(islice(line, j-4, j-2))
                    if tag == 'GSP':
                        gsp = ''.join(islice(line, j-4, j-2))
                    if tag == 'RPM':
                        rpm = ''.join(islice(line, j-4, j-2))
                    if tag == 'MOP':
                        mop = ''.join(islice(line, j-4, j-2))
                    if tag == 'ENL':
                        enl = ''.join(islice(line, j-4, j-2))
                    j = j+7

                if int(enl or 0) > 0:
                    eng = enl
                    sensor = "ENL"
                if int(mop or 0) > 0:
                    eng = mop
                    sensor = "MOP"
                if int(rpm or 0) > 0:
                    eng = rpm
                    sensor = "RPM"
                ilen = ''.join(islice(line, k-5, k-3))
                continue
            if igc == 'B':
                if (blen == 0):
                    blen = 1
                    if (ilen == 0):
                        ilen = len(line)
                if (len(line) != int(ilen)):
                    continue
                bcnt = bcnt + 1
                btime = atime
                blat = alat
                bN = aN
                blon = alon
                bW = aW
                bpress = apress
                bgnss = agnss
                atime = ''.join(islice(line, 1, 7))
            
                asec = ''.join(islice(atime, 4, 6))
                if (str(atime) == '000000'):
                    atime = btime
                    bcnt = bcnt - 1
                    continue
                aN = ''.join(islice(line, 14, 15))
                aW = ''.join(islice(line, 23, 24))
                if not (((aN == 'N') or (aN == 'S')) and ((aW == 'E') or (aW == 'W'))):
                    atime = btime
                    aN = bN
                    aW = bW
                    bcnt = bcnt - 1
                    continue
                alata = ''.join(islice(line, 7, 9))
                if ((int(alata) == 0) or (int(alata) > 90)):
                    atime = btime
                    bcnt = bcnt - 1
                    continue
                alatb = ''.join(islice(line, 9, 14))
                alatc = '{:0.5f}'.format(int(alatb) / 60000)
                alatd = str(alatc).split('.')
                alat = str(alata) + '.' + (str(alatd[1]))
                alona = ''.join(islice(line, 15, 18))
                alonb = ''.join(islice(line, 18, 23))
                alonc = '{:0.5f}'.format(int(alonb) / 60000)
                alond = str(alonc).split('.')
                alon = str(alona) + '.' + (str(alond[1]))
                aF = ''.join(islice(line, 24, 25))
                if aW == 'W':
                    alon = '-' + str(alon)
                if aN == 'S':
                    alat = '-' + str(alat)
                apnt = (float(alat), float(alon))
                bpnt = (float(blat), float(blon))
                if (float(blat) > 0):
                    dist = distance.distance(apnt, bpnt).m
                else:
                    dist = 0
                apress = ''.join(islice(line, 25, 30))
                agnss = ''.join(islice(line, 30, 35))
                if ((aF == 'V') or (int(dist or 0) > 5000) or (int(asec) == 60) or 
                    (int(apress) < -500) or (int(apress) == 0) or 
                    ((abs(int(bpress or 0)-int(apress))) > 800 and bcnt > 1)):
                    atime = btime
                    alat = blat
                    aN = bN
                    aW = bW
                    apress = bpress
                    agnss = bgnss
                    bcnt = bcnt - 1
                    continue
                if (bcnt == 1):
                    try:
                        dem = [demdata.index(float(alon), float(alat))]
                        demx = dem[0][0]
                        demy = dem[0][1]
                        demalt = dband1[demx, demy]
                    except IndexError:
                        demalt = 0
                    dpress = int(apress) - int(demalt)
                    if (int(dpress) > 150):
                        dpress = 0
                    print('Calculating Surface height and Alt offset at start Pressure Alt: ' 
                          + str(int(apress)) + '; Surface: ' + str(int(demalt)) 
                          + ' Offset: ' + str(int(dpress)))
                    spnt = (float(alat), float(alon))
                apress = int(apress) - int(dpress)
                if int(apress) > int(mpress):
                    mpress = apress
                if ((sginit == 0) and (int(apress) > 0)):
                    if dpress == 0:
                        spress = demalt
                    else:
                        spress = apress
                    sginit = 1
                paltd = int(apress) - int(bpress or 0)
                if btime != 0:
                    dtime = datetime.strptime(atime, FMT) - datetime.strptime(btime, FMT)
                    dsec = dtime.total_seconds()
                    if dsec == 0:
                        continue
                else:
                    dsec = 1
                pspd = spd
                if int(eng or 0) >= 1:
                    i = int(eng)-1
                    engval = ''.join(islice(line, i, i+3))
                if int(mop or 0) >= 1:
                    i = int(mop)-1
                    mopval = ''.join(islice(line, i, i+3))
                if int(enl or 0) >= 1:
                    i = int(enl)-1
                    enlval = ''.join(islice(line, i, i+3))
                if int(gsp or 0) >= 1:
                    i = int(gsp)-1
                    sp = ''.join(islice(line, i, i+3))
                    spd = int(sp) * K2M
                else:
                    spd = (dist / dsec) * M2F / 5280 * S2H if dsec != 0 else 0
                if (spd > (15*pspd)) and pspd != 0:
                    continue
                mslalt = M2F*float(apress)
                try:
                    dem = [demdata.index(float(alon), float(alat))]
                    demx = dem[0][0]
                    demy = dem[0][1]
                    demalt = int(M2F*dband1[demx, demy])
                except IndexError:
                    demalt = 0  # default ground elevation
                aglalt = mslalt - demalt
                if aglalt > maxaglalt:
                    maxaglalt = aglalt

                # Threshold-based MOP logic
                for t in thresholds:
                    if ((start != 0) and (int(mopval or 0) > t) and (mop_data[t]['mopflg'] == 0)):
                        mop_data[t]['mopflg'] = 1
                        mop_data[t]['smop'].append(atime)
                        mop_data[t]['mslmop'].append(mslalt)
                        mop_data[t]['aglmop'].append(int(aglalt))
                    if ((mop_data[t]['mopflg'] == 1) and (int(mopval or 0) < 50) and (int(mop or 0) > 0)):
                        mop_data[t]['mopflg'] = 0
                        mop_data[t]['smop'].append(atime)
                        mop_data[t]['mslmop'].append(mslalt)
                        mop_data[t]['aglmop'].append(int(aglalt))

                # ENL/RPM/MOP sensor logic
                if ((start != 0) and (int(engval or 0) > 0) and (engflg == 0)):
                    if (((sensor == "MOP") and (int(engval) > 500)) or
                        ((sensor == "ENL") and (int(engval) > 600)) or
                        ((sensor == "RPM") and (int(engval) > 50))):
                        engflg = 1
                        if (int(btime or 0) == int(start or 0)):
                            seng.append(start)
                            msleng.append(M2F*float(bpress))
                            agleng.append((M2F*float(bpress) - demalt))
                        else:
                            seng.append(atime)
                            msleng.append(mslalt)
                            agleng.append(aglalt)
                if (engflg == 1):
                    if (((sensor == "MOP") and (int(engval) < 50)) or 
                        ((sensor == "ENL") and (int(engval) < 250)) or 
                        ((sensor == "RPM") and (int(engval) < 20))):
                        engflg = 0
                        seng.append(atime)
                        msleng.append(mslalt)
                        agleng.append(aglalt)

                # ENL fallback
                if ((start != 0) and (int(enlval or 0) > 600) and (enlflg == 0) and (int(rpm or 0) == 0)):
                    enlflg = 1
                    senl.append(atime)
                    mslenl.append(mslalt)
                    aglenl.append(int(aglalt))
                if ((enlflg == 1) and (int(enlval or 0) < 250) and (rpm == 0)):
                    enlflg = 0
                    senl.append(atime)
                    mslenl.append(mslalt)
                    aglenl.append(int(aglalt))

                # Detect flight start
                if spd >= 35:
                    if start == 0:
                        start = atime
                        hpress = int(spress) + 180

            if btime == 0:
                continue

            # Detect flight stop
            if ((spd <= 15) and (aglalt <= 200) and (start != 0)):
                st = st + 1
                if st <= 5:
                    continue
                stop = atime
                ldist = distance.distance(spnt, bpnt).m
                if ldist > 1500:
                    lo = "LOUT"
                else:
                    lo = "HOME"
                fltime = datetime.strptime(stop, FMT) - datetime.strptime(start, FMT)
                flc = ''.join(islice(str(fltime), 1))
                if flc == '-':
                    res = str(fltime).split(",")
                    str(res[1]).split()
                    ftime = res[1]
                else:
                    ftime = fltime
                print('Glider: ' + str(gtype) + ' Date: ' + str(fdate) +
                      ' Flight Time: ' + str(ftime) + ' Landing: ' + str(lo) +
                      ' Start Alt: ' + str(int(M2F*int(spress))) + ' ft MSL')
                print('Start Time: ' + str(start) + ' Stop Time: ' + str(stop) +
                      ' Max Altitude: ' + str(int(M2F*int(mpress))) +
                      '[' + str(int(maxaglalt)) + '] ft MSL')

                # Engine run info
                i = 0
                while (i < len(seng)):
                    s = seng[i]
                    if (i+1 == len(seng)):
                        ss = atime
                        hgain = int(mslalt) - int(msleng[i])
                    else:
                        ss = seng[i+1]
                        hgain = int(msleng[i+1]) - int(msleng[i])
                    rntime = datetime.strptime(ss, FMT) - datetime.strptime(s, FMT)
                    runtime = round(rntime.total_seconds()) / 60
                    if (ma.floor(int(runtime)) > 0):
                        msg = (f"{gtype}'s {sensor} monitor reports Engine Run {int(runtime)} minutes, "
                               f"starts at T={s} and: {int(msleng[i])} msl [{int(agleng[i])} agl]; "
                               f"Height gain/loss is: {int(hgain)}")
                        print(msg)
                        sensor_info += msg + '\n'
                    i += 2

                # ENL info
                if (len(senl) > 0):
                    msg = f"{gtype} Motor noise registered by ENL sensor at t={senl} and {aglenl}AGL"
                    print(msg)
                    sensor_info += msg + '\n'

                # Threshold-based MOP info
                threshold_sensor_infos = {}
                for t in thresholds:
                    if (len(mop_data[t]['smop']) > 0):
                        msg = (f"{gtype} Motor noise registered by MOP sensor (threshold {t}) "
                               f"at t={mop_data[t]['smop']} and {mop_data[t]['aglmop']}AGL")
                        print(msg)
                        mop_data[t]['sensor_info'] += msg + '\n'
                    threshold_sensor_infos[t] = mop_data[t]['sensor_info'].strip()

                print('\n\n')

                # Extract the year from fdate
                try:
                    flight_year = fdate.split('/')[-1]
                except IndexError:
                    flight_year = 'Unknown'

                # Thread-safe CSV write
                with csv_lock:
                    if flight_year not in csv_writers:
                        output_file_path = f"Flt-times_{flight_year}.csv"
                        csv_file = open(output_file_path, "w", newline='', encoding='utf-8')
                        csv_writer = csv.writer(csv_file)
                        # Add extra columns for each threshold
                        header = [
                            'Date (MM/DD/YYYY)', 'File', 'Gtype', 'Flight Time', 
                            'Start Time', 'End Time', 'Landing', 'Sensor Info'
                        ]
                        for t in thresholds:
                            header.append(f"Sensor Info ({t})")
                        csv_writer.writerow(header)
                        csv_writers[flight_year] = {'writer': csv_writer, 'file': csv_file}
                    else:
                        csv_writer = csv_writers[flight_year]['writer']

                    row = [
                        str(fdate),
                        igcfn,  # <--- short file name
                        str(gtype),
                        str(ftime),
                        str(start),
                        str(stop),
                        lo,
                        sensor_info.strip()
                    ] + [threshold_sensor_infos[t] for t in thresholds]

                    csv_writer.writerow(row)

                # Reset variables for next flight
                HAT = onc = 0
                shat = []
                mhat = []
                that = []
                ahat = []
                atime = alat = alon = aN = aW = aF = apress = agnss = btime = 0
                spd = start = st = spress = bcnt = stop = hpress = mpress = sginit = 0
                eng = rpm = enl = mop = rpmval = enlval = mopval = engflg = enlflg = 0
                seng = []
                msleng = []
                agleng = []
                senl = []
                aglenl = []
                mslenl = []
                maxaglalt = aglalt = 0
                sensor_info = ''
                mop_data = {}
                for t in thresholds:
                    mop_data[t] = {
                        'mopflg': 0,
                        'smop': [],
                        'mslmop': [],
                        'aglmop': [],
                        'sensor_info': ''
                    }

        # End-of-trace check
        if ((stop == 0) and (start != 0)):
            print('End of Trace, No stop time found, print anyway')
            stop = atime
            fltime = datetime.strptime(stop, FMT) - datetime.strptime(start, FMT)
            ldist = distance.distance(spnt, bpnt).m
            if ldist > 1500:
                lo = "LOUT"
            else:
                lo = "HOME"
            flc = ''.join(islice(str(fltime), 1))
            if flc == '-':
                res = str(fltime).split(",")
                str(res[1]).split()
                ftime = res[1]
            else:
                ftime = fltime
            print('Glider: ' + str(gtype) + ' Date: ' + str(fdate) +
                  ' Flight Time: ' + str(ftime) + ' Landing: ' + str(lo) +
                  ' Start Alt: ' + str(int(M2F*int(spress))) + ' ft MSL')
            print('Start Time: ' + str(start) + ' Stop Time: ' + str(stop) +
                  ' Max Altitude: ' + str(int(M2F*int(mpress))) +
                  ' [' + str(int(maxaglalt)) + '] ft MSL')

            i = 0
            while (i < len(seng)):
                s = seng[i]
                if (i+1 == len(seng)):
                    ss = atime
                    hgain = int(mslalt) - int(msleng[i])
                else:
                    ss = seng[i+1]
                    hgain = int(msleng[i+1]) - int(msleng[i])
                rntime = datetime.strptime(ss, FMT) - datetime.strptime(s, FMT)
                runtime = round(rntime.total_seconds())/60
                if (ma.floor(int(runtime)) > 0):
                    msg = (f"{sensor} monitor reports Engine Run {int(runtime)} minutes, "
                           f"starts at T={s} and: {int(msleng[i])} msl [{int(agleng[i])} agl]; "
                           f"Height gain/loss is: {int(hgain)}")
                    print(msg)
                    sensor_info += msg + '\n'
                i += 2

            if (len(senl) > 0):
                msg = f"{gtype} Motor noise registered by ENL sensor at t={senl} and {aglenl}AGL"
                print(msg)
                sensor_info += msg + '\n'

            threshold_sensor_infos = {}
            for t in thresholds:
                if (len(mop_data[t]['smop']) > 0):
                    msg = (f"{gtype} Motor noise registered by MOP sensor (threshold {t}) "
                           f"at t={mop_data[t]['smop']} and {mop_data[t]['aglmop']}AGL")
                    print(msg)
                    mop_data[t]['sensor_info'] += msg + '\n'
                threshold_sensor_infos[t] = mop_data[t]['sensor_info'].strip()

            try:
                flight_year = fdate.split('/')[-1]
            except IndexError:
                flight_year = 'Unknown'

            # Thread-safe final CSV write
            with csv_lock:
                if flight_year not in csv_writers:
                    output_file_path = f"Flt-times_{flight_year}.csv"
                    csv_file = open(output_file_path, "w", newline='', encoding='utf-8')
                    csv_writer = csv.writer(csv_file)
                    header = [
                        'Date (MM/DD/YYYY)', 'File', 'Gtype', 'Flight Time',
                        'Start Time', 'End Time', 'Landing', 'Sensor Info'
                    ]
                    for t in thresholds:
                        header.append(f"Sensor Info ({t})")
                    csv_writer.writerow(header)
                    csv_writers[flight_year] = {'writer': csv_writer, 'file': csv_file}
                else:
                    csv_writer = csv_writers[flight_year]['writer']

                row = [
                    str(fdate),
                    igcfn,  # short file name
                    str(gtype),
                    str(ftime),
                    str(start),
                    str(stop),
                    lo,
                    sensor_info.strip()
                ] + [threshold_sensor_infos[t] for t in thresholds]

                csv_writer.writerow(row)

    except Exception as e:
        print(e)
        print('Exception occurred, go to next file')
        return
    finally:
        fn.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: flt-times.py [directory]/")
        sys.exit(0)

    # Dictionary to hold csv writers for each year
    csv_writers = {}

    # Load the DEM data once
    print("Adding DEM heights for each lat/long")
    demdata = rio.open('conus.tif')
    dband1 = demdata.read(1)

    # Gather all .IGC / .igc files from the directories
    all_igc_files = []
    for directory in sys.argv[1:]:
        igc_files = glob.glob(os.path.join(directory, "*.IGC")) \
                  + glob.glob(os.path.join(directory, "*.igc"))
        if not igc_files:
            print(f"No IGC files found in directory: {directory}")
        else:
            # Convert each found file to an absolute path
            for f in igc_files:
                all_igc_files.append(os.path.abspath(f))

    # Deduplicate paths (avoid processing same file multiple times)
    unique_files = list(set(all_igc_files))
    print(f"Found {len(unique_files)} unique IGC file(s).")

    if not unique_files:
        print("No IGC files found in any provided directories.")
        return

    # Process them in parallel with a thread pool
    max_workers = min(len(unique_files), os.cpu_count() or 1)
    print(f"Processing up to {max_workers} file(s) at a time...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(c_time, f, csv_writers, dband1, demdata): f
            for f in unique_files
        }
        for future in as_completed(future_to_file):
            file_ = future_to_file[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Error processing file {file_}: {exc}")

    # Close all open CSV files
    for year, writer_info in csv_writers.items():
        writer_info['file'].close()

    print("All done.")


if __name__ == "__main__":
    main()
