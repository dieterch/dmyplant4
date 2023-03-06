import dmyplant2
import traceback

global DEBUG
DEBUG = True 

def main():
    try:
        # load input data from files
        dval = dmyplant2.Validation.load_def_csv("Examples/out.csv")

        # ask & store credentials
        dmyplant2.cred()

        # myplant instance
        mp = dmyplant2.MyPlant(0) #parameter seconds to cache values e.g. 600 for 10 minutes or 0 to force reload

        # validation instance
        vl = dmyplant2.Validation(mp, dval, cui_log=False)

        # call dashboard
        d=vl.dashboard
        print(f'\nDashboard: \n{d}\n')

    except Exception as e:
        print(e)
        if DEBUG:
            traceback.print_tb(e.__traceback__)

if __name__ == '__main__':
    main()