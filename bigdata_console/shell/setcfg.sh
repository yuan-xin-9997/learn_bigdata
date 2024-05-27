#!/bin/sh
#…Ë÷√±‰¡ø

CFGFILE=$HOME/shell/sh.cfg
TempSh=/tmp/setcfg.$$

if [ $# -lt 2 ]
then
	print "Usage: $0 var value [configfilename]"
	exit
fi

Name=$1
shift 1
Value=$*

if [ -r $CFGFILE ]
then
	cat $CFGFILE|awk -F"=" -v Name=$Name -v Value="$Value" '
	BEGIN {
		flag=0
	}
	{
		if ( Name == $1 )
		{
			printf "%s=%s\n",$1,Value
			flag=1
		}
		else
		{
			for ( i=1 ;i<=NF;i++)
			{
				if (i>1)
					printf "=%s",$i
				else
					printf "%s",$i
			}
			printf "\n"
		}
	}
	END {
		if ( flag == 0 )
			printf "%s=%s\n",Name,Value
	}' > $TempSh
	mv $TempSh $CFGFILE
fi
