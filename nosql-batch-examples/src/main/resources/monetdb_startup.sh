#!/bin/sh

monetdbd start /mnt/bachir/dbfarm
monetdb create anx
monetdb release anx
