# I use python 3.6 from ananconda 
# To be able to run Kariz you should install
  - pip install connextion flask pandas numpy matplotlib 
  
# To run Kariz:
  - open file setup.sh and add the code path into your PYTOHN PATH
  - Make sure you would disable your firewall on port 3188 and 3187. Kariz deamon uses port 3188 and cache daemon uses port 3187.
  - Go to ${KARIZ_ROOT}/plans/kariz and run ./api/server.py
  - Go to ${KARIZ_ROOT}/cache and run ./server.py
  
  To test Kariz:
   - Go to simulator_framework folder. There in the tpc.py you could find several DAGs
   - To run all of the DAGs simple run ./main.py
