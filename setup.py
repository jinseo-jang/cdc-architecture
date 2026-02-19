import setuptools

setuptools.setup(
    name='cdc-architecture',
    version='0.0.1',
    description='CDC Dataflow Pipeline',
    install_requires=[
        'apache-beam[gcp]',
        'pyiceberg[pyarrow,gcp]',
        'google-auth',
        'requests',
        'pyarrow' 
    ],
    packages=setuptools.find_packages(),
)
