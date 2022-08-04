from setuptools import setup, find_packages

setup(
    name='test_package',
    version='1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'my_start=my_package.__main__:main',
        ]
    },
    install_requires=[
        'google-api-core==1.31.3',
        'google-auth==1.35.0',
        'google-cloud-bigquery==1.28.0',
        'google-cloud-core==1.7.2',
        'google-resumable-media==1.3.3',
        'googleapis-common-protos==1.52.0',
        'numpy==1.20.1',
        'pandas==1.2.4',
        'pytz==2021.1',
        'PyYAML==6.0'
    ]
)
