import contextlib
import requests
from tqdm import tqdm
from etldjango.settings import PROXI, PORT_PROXI, IP_PROXI_EXT
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(connect=5, backoff_factor=3.0)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


def urlretrieve(url, filename, n_bytes=1024):
    """
    url: origen source for direct download 
    filename: where to save the file and what is its name
    n_bytes: set the chunk size
    """
    if PROXI == 'yes':
        print('Internal proxy')
        proxies = dict(http='socks5://localhost:'+str(PORT_PROXI),
                       https='socks5://localhost:'+str(PORT_PROXI))
    elif not IP_PROXI_EXT is None:
        print('External proxy')
        proxies = dict(http='socks5://{}:'.format(IP_PROXI_EXT)+str(PORT_PROXI),
                       https='socks5://{}:'.format(IP_PROXI_EXT)+str(PORT_PROXI))
    else:
        print('NO proxy')
        proxies = None
    # proxies={"http": "http://201.234.60.82:999"}
    with contextlib.closing(session.get(url, stream=True, timeout=10, verify=True, proxies=proxies)) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=n_bytes):
                f.write(chunk)
    return filename, r.headers
