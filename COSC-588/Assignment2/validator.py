#!/usr/bin/env python
#
# validate student submission


import sys,zipfile

required = ["answer.txt"]
required += ["report%d.py" % i for i in range(1,5)]
required += ["report%d.txt" % i for i in range(1,5)]
optional = []
optional += ["join%d.py" % i for i in range(1,5)]
optional += ["join%d.txt" % i for i in range(1,5)]

required_files = 0
optional_files = 0
errors = 0

if __name__=="__main__":
    fname = sys.argv[1] if len(sys.argv)>1 else "ps02.zip"
    print("Validating {0}...\n".format(fname))
    z = zipfile.ZipFile(fname)
    for f in z.filelist:
        fname = f.orig_filename
        if fname in required:
            print("Required file {0} present.".format(fname))
            required.remove(fname)
            continue
        if fname in optional:
            print("Optional file: {0} present.".format(fname))
            optional.remove(fname)
            continue
        print("Unwanted file: {0}".format(fname))
        errors += 1

    print("")
    if required:
        print("MISSING REQUIRED FILES: "+" ".join(required))
        errors += len(required)
    if optional:
        print("MISSING OPTIONAL FILES: "+" ".join(optional))
    if errors:
        print("TOTAL ERRORS: {0}".format(errors))
    exit(errors)
