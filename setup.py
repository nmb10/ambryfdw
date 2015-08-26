from setuptools import setup

setup(
    name='ambryfdw',
    version='0.0.1',
    description='Ambry partitions msgpack files fdw.',
    url='http://github.com/FIXME:',
    author='Kazbek Byasov',
    license='Postgresql',
    packages=['ambryfdw'],
    install_requires=[
        'msgpack-python'  # ==0.4.6
    ],
    tests_require=['nose'],
    test_suite='nose.collector',
    zip_safe=False)
