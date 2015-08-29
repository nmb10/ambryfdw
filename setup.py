from setuptools import setup

setup(
    name='ambryfdw',
    version='0.0.1',
    description='Ambry partitions msgpack files Foreign Data Wrapper (postgresql, multicorn).',
    url='https://github.com/nmb10/ambryfdw',
    download_url='https://github.com/nmb10/ambryfdw/tarball/0.0.1',
    keywords=['ambry', 'fdw', 'ambry msgpack'],
    author='Kazbek Byasov',
    license='Postgresql',
    packages=['ambryfdw'],
    # install_requires versions should match to the ambry requires.
    install_requires=[
        'msgpack-python==0.4.6',
    ],
    tests_require=['nose', 'fudge'],
    test_suite='nose.collector',
    zip_safe=False)
