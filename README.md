memc_load

# Requirements

    Python 3+
        python-memcached
        protobuf
        
# Installation

git clone https://github.com/varusN/memc_load.git
pip install -r memc_load/requirements.txt

# Examples

* Dry run:

      python memc_load_multiprocessing.py --pattern="./*.tsv.gz" --dry

* Sample Output:
  
      python memc_load_multiprocessing.py --pattern=./tcs2/*.tsv.gz     
      [2023.12.11 01:03:10] I Memc loader started with options: {'test': False, 'log': None, 'dry': False, 'pattern': './tcs2/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:3
      3016'}
      [2023.12.11 01:03:11] I Processing ./tcs2\sample.tsv.gz
      [2023.12.11 01:03:11] I Processing ./tcs2\sample2.tsv.gz
      [2023.12.11 01:03:11] I Processing ./tcs2\sample3.tsv.gz
      [2023.12.11 01:03:13] I Acceptable error rate (0.0). Successfull loaded file ./tcs2\sample.tsv.gz
      [2023.12.11 01:03:13] I File ./tcs2\sample.tsv.gz is ready
      [2023.12.11 01:03:13] I Acceptable error rate (0.0). Successfull loaded file ./tcs2\sample2.tsv.gz
      [2023.12.11 01:03:13] I File ./tcs2\sample2.tsv.gz is ready
      [2023.12.11 01:03:13] I Acceptable error rate (0.0). Successfull loaded file ./tcs2\sample3.tsv.gz
      [2023.12.11 01:03:13] I File ./tcs2\sample3.tsv.gz is ready

