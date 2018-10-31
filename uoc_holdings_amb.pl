#!/usr/local/bin/perl 

#########################################################################
# set environment sub routine - standard routine

sub set_env {
	$TALIS_HOME = $ENV{"TALIS_HOME"}; 
	require "sybperl.pl"; 
	require "$TALIS_HOME/perl_tools/std_utils.pl";
}

#########################################################################
# create db connection - standard routine

sub db_connect {
	$Database = $_[0];
	&Std_open_db();
	&sql($d, "set dateformat dmy");
	&GetClientDateTime();
}
		
# sub routine: generate the results
sub dataset_query {

	my ($item_barcode, $work_id, $site, $edit_date, $class_number, $suffix, $title, $author, $pub_date, $sequence, $format, $order_number, $item_note, $status, $last_issue_date, $last_disch_site, $current_loan, $issue);
	my ($sql_bib_data, $result_row);
	my (@dt_bib_data);
	
	open (BIB_DATA, ">$output_file") or die("could not open file!");	
	
	select (BIB_DATA);
	$= = 50000;

	&db_connect("prod_talis");
	
	printf("\n");
	
	$sql_bib_data = "
		set			dateformat dmy
     
		select			I.ITEM_ID,
					I.WORK_ID,
					I.BARCODE,
					left (I.ACTIVE_SITE_ID, 4) as ACTIVE_SITE_ID,
					convert (varchar, I.EDIT_DATE, 103) as EDIT_DATE,
					(select rtrim(CLASS_NUMBER) from CLASSIFICATION where CLASS_ID = I.CLASS_ID) as CLASS_ID,
					rtrim (I.SUFFIX) as SUFFIX,
					(select substring(TITLE_DISPLAY, 5, 100) from WORKS where WORK_ID = I.WORK_ID) as TITLE,
					(select left(AUTHOR_DISPLAY, 100) from WORKS where WORK_ID = I.WORK_ID) as AUTHOR,
					(select NAME from ITEM_DESCRIPTION_TYPE where DESCRIPTION_ID = I.SEQUENCE_ID and SUB_TYPE = 1) as SEQUENCE,
					(select NAME from TYPE_STATUS where TYPE_STATUS = I.STATUS_ID and SUB_TYPE = 6) as DESCRIPTION,
					(select PUB_DATE from WORKS where WORK_ID = I.WORK_ID) as PUB_DATE,
					(select NAME from ITEM_DESCRIPTION_TYPE where DESCRIPTION_ID = I.FORMAT_ID and SUB_TYPE = 0) as FORMAT,
					(select ORE.ORDER_NUMBER from ORDER_REQUEST ORE, ITEM_ORDER_HISTORY IOH, ITEM IT where IOH.ORDER_ID = ORE.ORDER_ID and IOH.ITEM_ID = IT.ITEM_ID and IT.ITEM_ID = I.ITEM_ID) as ORDER_NUMBER,
					I.DESC_NOTE
		into			#TMP_ITEMS
		from			ITEM I
		where			(select CODE from TYPE_STATUS where TYPE_STATUS = I.STATUS_ID and SUB_TYPE = 6) in ('IS', 'SALE', 'REP', 'PROC', 'ONLY', 'ISCN', 'CAT') and
					-- I.STATUS_ID = 5 and
					-- I.CREATE_DATE >= '01/08/2011' and
					I.ACTIVE_SITE_ID = 'AMB'	

		select			L.LOAN_ID,
					L.ITEM_ID,
					L.CURRENT_LOAN,
					L.STATE
		into			#TMP_LOANS
		from			#TMP_ITEMS TI,
					LOAN L,
					BORROWER B
		where			L.ITEM_ID = TI.ITEM_ID and
					L.BORROWER_ID = B.BORROWER_ID and
					(select CODE from TYPE_STATUS where SUB_TYPE in (2) and TYPE_STATUS = B.TYPE_ID) not in ('TMP', 'ZZZZ', 'TBB', 'RM', 'RES', 'DUM') and
					L.CREATE_DATE between '$start_date' and '$end_date'
    
		select			TPL.ITEM_ID,
					convert (varchar, (select max (CREATE_DATE) from LOAN where ITEM_ID = TPL.ITEM_ID and STATE = 2), 103) as LAST_DISCH,
					(select max (CREATE_LOCATION) from LOAN where ITEM_ID = TPL.ITEM_ID and STATE = 2) as DISCH_SITE,
					(select count (LOAN_ID) from #TMP_LOANS where ITEM_ID = TPL.ITEM_ID and STATE = 0) as ISSUE_COUNT,
					case (select max (CURRENT_LOAN) from LOAN where ITEM_ID = TPL.ITEM_ID) 
						when 'T' then 'on loan'
						when 'F' then 'discharged'
						else 'never loaned'
					end as CURRENT_LOAN
		into			#TMP_LOAN_INFO
		from			#TMP_LOANS TPL
		group by		TPL.ITEM_ID
   
		select			TI.BARCODE,
					TI.WORK_ID,
					TI.ACTIVE_SITE_ID,
					TI.EDIT_DATE,
					TI.CLASS_ID,
					TI.SUFFIX,
					TI.TITLE,
					TI.AUTHOR,
					TI.PUB_DATE,
					TI.SEQUENCE,
					TI.FORMAT,
					TI.ORDER_NUMBER,
					TI.DESC_NOTE,
					TI.DESCRIPTION,
					TLI.LAST_DISCH,
					TLI.DISCH_SITE,
					TLI.CURRENT_LOAN,
					TLI.ISSUE_COUNT
                from			#TMP_ITEMS TI
		left outer join		#TMP_LOAN_INFO TLI
		on			TI.ITEM_ID = TLI.ITEM_ID
		order by		TI.CLASS_ID, TI.SUFFIX
			        
		drop table    		#TMP_ITEMS
		drop table		#TMP_LOANS
		drop table		#TMP_LOAN_INFO
		";
		
		@dt_bib_data = &sql($d, $sql_bib_data);
		
		foreach $result_row (@dt_bib_data) {
			$result_row =~ s/[^A-Za-z0-9 ,:~;.\/]//g;
			($item_barcode, $work_id, $site, $edit_date, $class_number, $suffix, $title, $author, $pub_date, $sequence, $format, $order_number, $item_note, $status, $last_issue_date, $last_disch_site, $current_loan, $issue) = &despace(split("~", $result_row));
			# ($item_barcode, $work_id, $site, $edit_date, $class_number, $suffix, $title, $author, $pub_date, $sequence, $format, $order_number, $item_note, $status, $last_issue_date, $last_disch_site, $current_loan, $issue) = &despace(split("~", lc($result_row)));
			$~ = BIB_DATA;
			write BIB_DATA; 
		}
	
		close (BIB_DATA, ">$filename");
		
		select (STDOUT);
	
format BIB_DATA_TOP =
"site","item barcode","bib id","class number","suffix","title","author","pub date","sequence","format","order number","item note field","status","edit date","last disch date","discharge site","current loan","issue count"
.
format BIB_DATA =
"@<<<<",="@<<<<<<<<<<<<<<",="@<<<<<<<<<<<<<<<",="@<<<<<<<<<<<<<<<","@<<<","@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<","@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<<<<<<<<<<<<<","@<<<<<<<<<<<","@<<<<<<<<<<<","@<<<<<<<<<<<","@<<<<<<<<<<<","@<<<"
$site, $item_barcode, $work_id, $class_number, $suffix, $title, $author, $pub_date, $sequence, $format, $order_number, $item_note, $status, $edit_date, $last_issue_date, $last_disch_site, $current_loan, $issue
.
}

#############################################################################
# main part of script

sub main {
	# declare variables and set scope
	local ($script) = "holdings at ambleside";
	local ($output_file) = "output/amb_holdings.csv";
	local ($day, $month, $year) = 0;
	
	local ($start_date) = "01/08/2010";
	
	($day, $month, $year) = (localtime)[3, 4, 5];
 	# local ($end_date) = printf("%02d/%02d/%04d", $day, ($month + 1), ($year + 1900));
  	$year = $year + 1900; $month++;
 	$end_date = "$day/$month/$year";
	
	printf ("passed output file is 		%-16s\n", $output_file);
	printf ("\n");
	printf ("begin				%-16s\n", $script);
	printf("press any key to continue or press <ctrl> c to quit\n");
	<STDIN>;
	printf ("beginning			%-16s\n", $script);
	
	&set_env;
	&dataset_query;
}

#############################################################################
          
use strict;

eval 'exec /usr/local/bin/perl -S $0 ${1+"$@"}'
	if 0;
	
&main;
	
exit;
