import logging as log
import os

log.basicConfig(level=os.environ.get("LOGLEVEL"), format='%(asctime)s %(levelname)s %(message)s')
