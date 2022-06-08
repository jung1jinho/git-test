# from cli import cli, clip
import cli

def sche_conf():
    cli.cli("configure terminal; \
        terminal monitor")
    cli.cli("show interface eth1/1")


if __name__=='__main__':
    sche_conf()