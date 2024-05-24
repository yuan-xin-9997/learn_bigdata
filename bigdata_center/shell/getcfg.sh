#!/bin/sh

if [ "$SHELLPATH" = "" ]
then
	if [ $# -gt 1 ]
	then
		CFGFILE=shell/console/$2
	else
		CFGFILE=shell/sh.cfg
	fi
else
	if [ $# -gt 1 ]
	then
		CFGFILE=$SHELLPATH/console/$2
	else
		CFGFILE=$SHELLPATH/sh.cfg
	fi
fi

TempSh=/tmp/getcfg.`whoami`.$$


if [ $# -lt 1 ]
then
	printf ""
fi

argname=$1
osoperator=`uname -s`

getcfg()
{
if [ -r $CFGFILE ]
then
        grep -v "^#" $CFGFILE|awk -v argname=$argname -v osoperator=$osoperator -F"=" '
        BEGIN {
                argvalue=""
                i=0
        }
        {
                if ( $1 != "" )
                {
                        a_name[i]=$1
                        a_value[i++]=$2
                }
                if (substr(argname,1,1) == "@")
                {
                        for ( j=0;j<i;j++ )
                        {
                                len=length(a_name[j])
                                if(a_name[j]==substr(argname,2,len))
                                {
                                        argname=sprintf("%s%s",a_value[j],substr(argname,len+2,length(argname)-len-1))
                                        break
                                }
                        }
                }

                if ( argname == $1 )
                        argvalue=$2

                if (substr(argvalue,1,1) == "@")
                {
                        for ( j=0;j<i;j++ )
                        {
                                len=length(a_name[j])
                                if(a_name[j]==substr(argvalue,2,len))
                                {
                                        argvalue=sprintf("%s%s",a_value[j],substr(argvalue,len+2,length(argvalue)-len-1))
                                        break
                                }
                        }
                        if ( j==i)
                        {
                                arvvalue=sprintf("$%s",substr(argvalue,2,length(argvalue)-1))
                        }
                }
        }
        END {
		if ( osoperator == "Linux" ) 
			printf "printf \"\\\"%s\\\"\"",argvalue
		else if ( osoperator == "HP-UX" )
			printf "printf \"%s\"",argvalue	
			else
				printf "echo OS error!"

                if ( argvalue == "" )
                        printf "\nexit -1"
        }'> $TempSh
        chmod +x $TempSh
	if [ "$osoperator" = "Linux" ]
	then
		$TempSh |sed 's/\"//g'
	else
		$TempSh
	fi
        ret=$?
fi	
}

getcfg
#rm -rf $TempSh 2>/dev/null
exit $ret
