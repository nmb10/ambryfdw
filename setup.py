from setuptools import setup

setup(
    name='ambryfdw',
    version='0.0.1',
    description='Ambry partitions msgpack files fdw.',
    url='https://github.com/nmb10/ambryfdw',
    author='Kazbek Byasov',
    license='Postgresql',
    packages=['ambryfdw'],
    # Install requires versions should match to the ambry requires.
    install_requires=[
        'msgpack-python==0.4.6',
    ],
    tests_require=['nose', 'fudge'],
    test_suite='nose.collector',
    zip_safe=False)
