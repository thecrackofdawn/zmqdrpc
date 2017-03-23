from setuptools import setup

setup(name="zmqdrpc",
    version='0.3',
    description = "distributed rpc based on zeromq",
    author = "thecrackofdawn",
    author_email = "shallweqin@gmail.com",
    url = "",
    packages = ["zmqdrpc"],
    install_requires = ["pyzmq", "msgpack-python"]
    )
