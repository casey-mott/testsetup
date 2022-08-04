from setuptools import setup, find_packages

setup(
    name='casey_test_package',
    version='1.5.3',
    packages=find_packages(),
    install_requires=[
        'google-api-core==1.31.3',
        'google-auth==1.35.0',
        'google-cloud-bigquery==1.28.0',
        'google-cloud-core==1.7.2',
        'google-resumable-media==1.3.3',
        'googleapis-common-protos==1.52.0',
        'numpy>1.16',
        'pandas>0.23',
        'pytz==2021.1',
        'PyYAML>5.3'
    ]
)
