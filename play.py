import dmyplant2
from pprint import pprint as pp
import time
t0 = time.time()
dmyplant2.cred()
mp = dmyplant2.MyPlant(0)
#dmyplant2.Engine._list_cached_validations()
t1 = time.time()

#e = dmyplant2.Engine.from_sn(mp, '1486144')
t2 = time.time()
#pp(e.dash)
print(f"Login: {(t1-t0):3.2f}sec\nEngine: {(t2-t1):3.2f}sec")
#r = mp._asset_data_graphQL(assetId=159396)
#print(r)