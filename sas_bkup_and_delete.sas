/*run as Remote Submit*/
LIBNAME GLOBAL SASSPDS 
    IP=YES 
    LIBGEN=NO 
    HOST="yourhost"  /* Replace with your host alias */
    Serv="yourservice"     /* Replace with your service alias */
    SCHEMA="GLOBAL" /* Replace with your schema alias */
    AUTHDOMAIN="yourAuth"; /* Replace with your authdomain alias */

/*run as local submit*/
libname GLOBAL server=%sysfunc(getoption(remote));
SNA

/*run as Remote Submit*/
LIBNAME GLOBAL SASSPDS IP=YES LIBGEN=NO HOST="yourhost" Serv="yourservice" SCHEMA="GLOBAL" AUTHDOMAIN="SPDSAuth" ;

/*run as local submit*/
libname GLOBAL server=%sysfunc(getoption(remote));

data Global.newtablname;
set Global.yourtablename;
run;

/*delete older tables*/

proc delete data = Global.yourtablename;
run;

