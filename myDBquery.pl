#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Std;

#
# Provides basic query interfaces for myDB.  Queries provided are :
#
# perl myDBquery.pl [-s option[,option]] [-o option[,option]] [-f option=value]
#
#   -s		specify which record fields of the query result to display
#   -o		specify which record fields to sort the query results by
#   -f		specify a record field and value to filter the query results
#
# option	a valid data field tag...STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME
# value		a valid value for the specified data field
#



my $DATA_STORE_DIR = ".\\datastore\\";			# path to datastore directory
my $NUM_REC_FIELDS = 7;					# number of fields in a datastore record
my $NUM_DATA_FIELDS = $NUM_REC_FIELDS - 1;		# number of datas fields in a datastore record (minus the `valid` meta field)

# record field indices
# used to match datastore record fields to filter elements
my $field_stb       = 0;
my $field_title     = 1;
my $field_provider  = 2;
my $field_date      = 3;
my $field_rev       = 4;
my $field_view_time = 5;

# Select filter to determine which record fields are printed out.
# An array whose elements represent the fields to print out and their respective order.
# If `select_filter` is {3, 4, 1, -1}, then the query should display the fourth, fifth
# and second data fields, in that order.
my @select_filter;

# Filter to select criteria for accepting records.
# An array whose elements represent values on which to filter the data records to display.
# If `input_filter` is {-1, -1, -1, 2014-04-01, -1, -1} then only data records whose fourth
# data field is `2014-04-01` should be returned by the query.
my @input_filter;
# did the user specify a filter?
my $input_filter_enabled = 0;

# Order filter for determining print order of records.
# An array whose elements represent the fields on which the query results should be
# sorted.  If `order_filter` is {3, 1, -1} then the output should first be sorted by
# the value of the fourth data field, and then by the value of the second data field.
my @order_filter;
# did the user specify an order?
my $order_filter_enabled = 0;
# how many order values did the user specify?
my $num_order_specified = 0;

# list of files in the datastore directory
my @data_store_files;

# command line options
my %opts;

# an array of array refs used to store data records for sorting
my @refs_array;


# print out correct usage and exit
sub usage {
	print("\nperl myDBquery.pl [-s option[,option]] [-o option[,option]] [-f option=value]\n");
	print("\n");
	print("   -s            specify which record fields of the query result to display\n");
	print("   -o            specify which record fields to sort the query results by\n");
	print("   -f            specify a record field and value to filter the query results\n");
	print("\n");
	print("   option        a valid data field tag...STB, TITLE, PROVIDER, DATE, REV, VIEW_TIME\n");
	print("   value         a valid value for the specified data field\n");
	
	exit 1;
}


# Translate a string provided on the command line to the proper filter index.
# Used to set filter values based on command line options.
sub fieldNameToIndex {
	my $name = shift;
	
	if ($name eq "STB") {
		return 0;
	} elsif ($name eq "TITLE") {
		return 1;
	} elsif ($name eq "PROVIDER") {
		return 2;
	} elsif ($name eq "DATE") {
		return 3;
	} elsif ($name eq "REV") {
		return 4;
	} elsif ($name eq "VIEW_TIME") {
		return 5;
	} else {
		# invalid string passed in by user
		usage();
	}
}


# initialize filters to default values
sub init {
	# Initialize to default output order
	@select_filter = map 1*$_, 0..($NUM_DATA_FIELDS - 1);
	
	# "-1" means no filter...may need a new token if any data fields
	# can equal "-1", such as STB
	for (my $i = 0; $i < $NUM_DATA_FIELDS; $i++) {
		$input_filter[$i] = "-1";
	}
	
	# Initialize to default sort order
	@order_filter = map 1*$_, 0..($NUM_DATA_FIELDS - 1);
}


# configure query based on command line options
sub processOpts() {
	if (defined $opts{f}) {
		my @f_opt_fields = split /=/, $opts{f};
		
		# only support one filter value
		if (scalar @f_opt_fields != 2) {
			print("Illegal -f format\n");
			usage();
		}
		
		# set input_filter entry corresponding to the options used
		$input_filter[fieldNameToIndex($f_opt_fields[0])] = $f_opt_fields[1];
		$input_filter_enabled = 1;
	}
	
	if (defined $opts{o}) {
		my @o_opt_fields = split /,/, $opts{o};
		
		# do not accept more order values than there are data fields
		if (scalar @o_opt_fields > $NUM_DATA_FIELDS) {
			die "Too many values specified for -o";
		}
		
		my $index = 0;
		for my $o_opt (@o_opt_fields) {
			# set order_filter entries corresponding to the options used
			$order_filter[$index++] = fieldNameToIndex($o_opt);
			
			# remember how many order values were specified
			$num_order_specified++;
		}
		
		# -1 for end of formatting
		$order_filter[$index] = -1;
		$order_filter_enabled = 1;
	}
	
	if (defined $opts{s}) {
		my @s_opt_fields = split /,/, $opts{s};
		
		# do not accept more select values than there are data fields
		if (scalar @s_opt_fields > $NUM_DATA_FIELDS) {
			die "Too many values specified for -s";
		}
		
		my $index = 0;
		for my $s_opt (@s_opt_fields) {
			# set select_filter entries corresponding to the options used
			$select_filter[$index++] = fieldNameToIndex($s_opt);
		}
		
		# -1 for end of formatting
		$select_filter[$index] = -1;
	}
}


# get all files in the datastore directory
# Returns : array of file names in $DATA_STORE_DIR
sub getDSFiles {
	my @data;
	
	opendir my $dir, $DATA_STORE_DIR or die "Cannot open directory: $!";
	@data = readdir $dir;
	closedir $dir;
	
	return @data;
}


# Make sure the file name we are using as a datastore file has a name
# which conforms to our expected naming convention.
sub validateFileName {
	my $file_name = shift;
	
	if ($file_name =~ /^ds[0-9]+.txt/) {
		return 1;
	}
	
	print("Warning: Invalid file found $file_name\n");
	
	return 0;
}


# Break the record we read from the datastore file into fields and format it.
#   - make sure the number of fields we get is the expected number
#   - remove the first field, which is a meta-field, not data, we already used it
#   - remove newline char from last field
#
# Input   : string of raw datastore data
# Returns : array of record fields
sub breakRecIntoFields {
	my $record = shift;
	
	# split file record into fields
	my @rec_fields = split /\|/, $record;
		
	# validate number of fields
	if (scalar @rec_fields != $NUM_REC_FIELDS) {
		die "Read illegal field from datastore file - $record";
	}
		
	# remove `valid` field, we no longer need it
	shift @rec_fields;
		
	# get rid of newline char in last field
	$rec_fields[$NUM_DATA_FIELDS - 1] =~ s/\n//;
	
	return @rec_fields;
}


# This code is setup to accept multiple filters even though
# the rest of the code does not yet support that.
#
# Input  : array of record fields
# Return : 1 if record input matches input_filter requirements
#          0 if not
sub filterInput {
	my @rec = @_;
	
	for (my $i = 0; $i < $NUM_DATA_FIELDS; $i++) {
		if ($input_filter[$i] ne "-1") {
			if ($input_filter[$i] ne $rec[$i]) {
				return 0;
			}
		}
	}
	
	return 1;
}


# print query results based on select_filter setting
#
# Input : reference to an array of record fields
sub outputResults {
	my $rec_ref = shift;
	my $first = 1;
	
	for my $i (@select_filter) {
		if ($i == -1) {
			last;
		}
		if ($first) {
			print("@$rec_ref[$i]");
			$first = 0;
		} else {
			print(",@$rec_ref[$i]");
		}
		
	}
	print("\n");
}


# Use order_filter settings to sort our array of records.
# I could not figure out a way to dynamically add conditions to the
# sort call so for brevity, I simply hard-coded the possibilities.
#
# Input  : reference to an array of record array references
#               basically all the records we have found to match the query
# Return : reference to a sorted array of record array references
sub sortOutput {
	my $refs_arr_ref = shift;
	my @sorted_refs_arr;
	
	if ($num_order_specified == 1) {
		@sorted_refs_arr = sort { $a->[$order_filter[0]] cmp $b->[$order_filter[0]] } @$refs_arr_ref;
	} elsif ($num_order_specified == 2) {
		@sorted_refs_arr = sort { $a->[$order_filter[0]] cmp $b->[$order_filter[0]]
		                          || $a->[$order_filter[1]] cmp $b->[$order_filter[1]] } @$refs_arr_ref;
	} elsif ($num_order_specified == 3) {
		@sorted_refs_arr = sort { $a->[$order_filter[0]] cmp $b->[$order_filter[0]]
		                          || $a->[$order_filter[1]] cmp $b->[$order_filter[1]]
		                          || $a->[$order_filter[2]] cmp $b->[$order_filter[2]] } @$refs_arr_ref;
	} elsif ($num_order_specified == 4) {
		@sorted_refs_arr = sort { $a->[$order_filter[0]] cmp $b->[$order_filter[0]]
		                          || $a->[$order_filter[1]] cmp $b->[$order_filter[1]]
		                          || $a->[$order_filter[2]] cmp $b->[$order_filter[2]]
		                          || $a->[$order_filter[3]] cmp $b->[$order_filter[3]] } @$refs_arr_ref;
	} elsif ($num_order_specified == 5) {
		@sorted_refs_arr = sort { $a->[$order_filter[0]] cmp $b->[$order_filter[0]]
		                          || $a->[$order_filter[1]] cmp $b->[$order_filter[1]]
		                          || $a->[$order_filter[2]] cmp $b->[$order_filter[2]]
		                          || $a->[$order_filter[3]] cmp $b->[$order_filter[3]]
		                          || $a->[$order_filter[4]] cmp $b->[$order_filter[4]] } @$refs_arr_ref;
	} else {
		@sorted_refs_arr = sort { $a->[$order_filter[0]] cmp $b->[$order_filter[0]]
		                          || $a->[$order_filter[1]] cmp $b->[$order_filter[1]]
		                          || $a->[$order_filter[2]] cmp $b->[$order_filter[2]]
		                          || $a->[$order_filter[3]] cmp $b->[$order_filter[3]]
		                          || $a->[$order_filter[4]] cmp $b->[$order_filter[4]]
		                          || $a->[$order_filter[5]] cmp $b->[$order_filter[5]] } @$refs_arr_ref;
	}
	
	my $sorted_refs_arr_ref = \@sorted_refs_arr;

	return $sorted_refs_arr_ref;
}



#############
# MAIN
#############

# initialize filters
init();

# process command line options
getopts('f:o:s:', \%opts) or usage();
processOpts();

# get datastore files in datastore directory
@data_store_files = getDSFiles();

# - foreach file found in the data stopre directory
#	- validate file name
# 	- open file
#	- foreach line of data read from the file
#		- ignore invalidated datastore records
# 		- break the raw datastore data intro an array of record fields
# 		- filter input based on `input_filter` settings
# 		- if user requested ordering, add record data to list of record data
# 		- else print query results based on `select_filter` settings
#	- close file
# - if user requested ordering
#	- sort record data according to `order_filter`
#	- print query results based on `select_filter` settings
for my $DSFile (@data_store_files) {
	# ignore "." and ".."
	if (($DSFile eq ".") || ($DSFile eq "..")) {
		next;
	}
	
	if (!validateFileName($DSFile)) {
		next;
	}
	
	open(my $DSF, "<", "$DATA_STORE_DIR\\$DSFile") or die "Can't open datastore file $DSFile: $!";
	while (my $rec = <$DSF>) {
		# ignore invalid records
		if ($rec =~ /^0/) {
			next;
		}
		
		# split file record into fields
		my @rec_fields = breakRecIntoFields($rec);
		
		# check `input_filter` settings if they have been specified
		if ($input_filter_enabled) {
			if (!filterInput(@rec_fields)) {
				next;
			}
		}
		
		if ($order_filter_enabled) {
			# must add rec to array ref
			my $rec_ref = \@rec_fields;
			push(@refs_array, $rec_ref);
		} else {
			# if we are not sorting, we can just output the record
			my $rec_fields_ref = \@rec_fields;
			outputResults($rec_fields_ref);
		}
	}
	close $DSF;
}

# sort and then display query results
if ($order_filter_enabled) {
	my $refs_array_ref = \@refs_array;
	my $sorted_refs_array_ref = sortOutput($refs_array_ref);

	for my $rec_arr_ref (@$sorted_refs_array_ref) {
		outputResults($rec_arr_ref);
	}
}



