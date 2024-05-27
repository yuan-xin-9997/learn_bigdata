#!/bin/sh

if [ $# -lt 2 ]
then
	echo usage: "$0 -grp groupname | -icol colname=colvalue |-iicol colname=colvalue| -ocols colname,colname,... -n"
	exit 1
fi


argv=""
while [ $# -gt 0 ]
do
	case $1 in
	-grp)
		argv=${argv}" -v grp=$2"
		shift 2
		;;
	-igrp)
		argv=${argv}" -v igrp=$2"
		shift 2
		;;
	-icol)
		sarg=`echo $2|awk -F"=" '{printf "-v icolname=%s -v icolvalue=%s",$1,$2}'`
		argv=${argv}" ${sarg}"
		shift 2
		;;
	-iicol)
		sarg=`echo $2|awk -F"=" '{printf "-v iicolname=%s -v iicolvalue=%s",$1,$2}'`
		argv=${argv}" ${sarg}"
		shift 2
		;;
	-ocols)
		argv=${argv}" -v ocolnames=$2"
		shift 2
		;;
	-n)
		argv=${argv}" -v notitle=true"
		shift 1
		;;
	-g)
		argv=${argv}" -v grpdisp=true"
		shift 1
		;;
	*)
		;;
	esac
done

awk $argv '
	function regMatch(reg,str)
	{
		split(reg,a,"/")
		for(i=1;;i++)
		{
			if(a[i] == "")
			{
				break; 
			}
			if(a[i] == str)
			{ 
				return 1
			}      
		} 
		return 0
	}
	BEGIN {
		icolno=0
		iicolno=0
		ocolmax=0
		group=""
		str=ocolnames
		j=1
		pos=0
		do
		{
			pos=index(str,",")
			if (pos==0)
			{
				ocolnms[j]=str
			}
			else
			{
				ocolnms[j]=substr(str,1,pos-1)
				str=substr(str,pos+1,length(str)-pos)
			}
			j++
		} while (pos!=0)
		ocolmax=j

	}
	{
		isNote="false"
		pres=substr($1,1,2)
		if ( pres == "##" )
		{
			flag=0
			for (i=1;i<=NF;i++)
			{

				if (i==1)
				{
					scol=substr($1,3,length($1)-2)
				}
				else
				{
					scol=$i
				}

				for(m=1;m<ocolmax;m++)
				{
					if(scol==ocolnms[m])
						ocols[i]=1
				}

				if (scol==icolname)
				{
					icolno=i
				}

				if (scol==iicolname)
				{
					iicolno=i
				}

				if ((ocolnames==""||ocols[i]==1)&&notitle=="")
				{
					if(flag==0&&i>1)
						printf ("##")
					flag++
					printf ("%s ",$i)
				}
			}
			if (flag>0)
			{
				printf ("\n")
			}
			isNote="true"
		}
		else if (substr($1,1,1)=="#")
		{
			isNote="true"
		}
		else if (substr($1,1,1)=="[")
		{
			group=substr($1,2,length($1)-2)
			isNote="true"
			if (grpdisp=="true")
				printf "[%s]\n",group
		}

		isShow="false"
		if ( (igrp == "" || !regMatch(igrp,group)) && (group=="" || group=="all"||grp==""||grp=="all"||regMatch(grp,group)))
		{
			isShow="true"
		}

		if (icolname!="" && icolno>0 && !regMatch(icolvalue,$icolno) && icolvalue !="all")
		{
			isShow="false"
		}

		if (iicolname!="" && iicolno>0 && regMatch(iicolvalue,$iicolno))
		{
			isShow="false"
		}

		if ( isShow == "true" && isNote != "true")
		{
			flag=0
			for (i=1;i<=NF;i++)
			{
				if (ocolnames=="" || ocols[i]==1)
				{
					if (flag > 0)
						printf (" %s",$i)
					else
						printf ("%s",$i)
					flag++
				}
			}
			if (flag>0)
				printf "\n"
		}
	}'		

