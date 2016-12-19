/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  	
  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  count = 0; // count number of matching tuples

  IndexCursor cur;	// New variable: Navigating the tree
  BTreeIndex tree; // Creating an index if index file available

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
	fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
	return rc;
  }
  
	/* START: Dummy flag variables for evaluating select condition expressions */

	bool indexFlag = false; // to check if index file needs to be closed
	bool condFlag = false; // to check if there is any valid condition
	bool valueCondFlag = false; // to check if there is any value condition (for speed up)
	bool valueMismatch = false; // to check if two conditions conflict

	/* Flags for comparators */

	int equalVal = -1; // initialize to any negative number. Doesn't really matter
	int Eflag = 0; // 0 indicates no equality condition, set to 1 otherwise
	int GEflag = 0; 
	int LEflag = 0; 
	int minVal = -1;
	int maxVal = -1;
	bool withoutIndex = false;
		 
	int conditionNumber = -1; // track valid conditions
	
	/* END: Dummy variables for evaluating select condition expressions */
	
	// check all select conditions to draw conclusions for further processing
	for(int i=0; i<cond.size(); i++)
	{
		/* Note: Each condition has 3 params: (a) attr (1: key, 2: value) (b) comp (EQ, GT, etc)
		and (c) comparison value (char*) */

		int keyValue = atoi(cond[i].value); // get integer equivalent of the comparison value.
		 
		// As mentioned, if condition is <> (NE), Index will not be used.

		if(cond[i].attr==1 && cond[i].comp!=SelCond::NE) // for keys not involving NE
		{
			condFlag = true; // atleast one valid condition found.
			
			// Next, we figure out what exactly is the comparator one by one.
			switch(cond[i].comp)
			{
				case SelCond::EQ: // EQ decides the entire condition => break now.
				conditionNumber = i; // store the condition number to work on
				equalVal = keyValue; // now equalVal != -1
				Eflag = 1; // not 0 anymore
				break; // we are done if attr = 1
			
				case SelCond::GE: // keep updating the minVal value constraint on keys
				if(minVal==-1 || keyValue > minVal) 
				{
					GEflag = 1;
					minVal = keyValue;
				}
				break;

				case SelCond::GT: // keep updating the minVal value constraint on keys
				if(minVal==-1 || keyValue >= minVal)
				{
					GEflag = 0;
					minVal = keyValue;
				}
				break;

				case SelCond::LE: // keep updating the maxVal value constraint on keys
				if(maxVal==-1 || keyValue < maxVal) 
				{
					LEflag = 1;
					maxVal = keyValue;
				}
				break;
				
				case SelCond::LT: // keep updating the maxVal value constraint on keys
				if(maxVal==-1 || keyValue <= maxVal) 
				{
					LEflag = 0;
					maxVal = keyValue;
				}
				break;
			}

		}
		else if(cond[i].attr==2) // attr = 2 for value
		{
			valueCondFlag = true; // value condition found
			
			if(cond[i].comp==SelCond::EQ) //check on value equality
			{
				string valEq = ""; // dummy used as ""
				if(strcmp(value.c_str(), cond[i].value)==0) valEq=keyValue;
				// if value is same as specified earlier => OK
				else if(valEq=="") valEq = keyValue;
				// if there is no value specified => OK!
				else valueMismatch = true;
				// mismatch in values => Not OK! => won't return any values
			}
		}
	}
	
	// check if select conditions make sense. if they conflict, we return 0 tuples.

	bool cond1 = (maxVal!=-1 && minVal!=-1 && !GEflag && !LEflag && maxVal==minVal);
	bool cond2 = (valueMismatch==true);
	bool cond3 = (maxVal!=-1 && minVal!=-1 && maxVal<minVal);
	
	if(cond1 || cond2 || cond3)
		goto condition_unmet;

  /* When NOT to use IndexTree ?

  1. Index File not available.
  2. NE Condition on key
  3. No other select condition and expression is not 'select count(*) from tablename'*/

  withoutIndex = (!condFlag && attr!=4); // covers condition 2 and 3 above.

  if(tree.open(table + ".idx", 'r')!=0 || withoutIndex)
  {
  	// cout<<"Index File Not Found OR Condition not specified"<<endl;
  	// same code as provided earlier
	// scan the table file from the beginning
	  rid.pid = rid.sid = 0;
	  count = 0;
	  while (rid < rf.endRid()) {
		// read the tuple
		if ((rc = rf.read(rid, key, value)) < 0) {
		  fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
		  goto exit_select;
		}

		// check the conditions on the tuple
		for (unsigned i = 0; i < cond.size(); i++) 
		{
		  // compute the difference between the tuple value and the condition value
		  switch (cond[i].attr) 
		  {
		  case 1:
			diff = key - atoi(cond[i].value);
			break;
		  case 2:
			diff = strcmp(value.c_str(), cond[i].value);
			break;
		  }

		  // skip the tuple if any condition is not met
		  switch (cond[i].comp) 
		  {
			  case SelCond::EQ:
				if (diff != 0) goto next_tuple;
				break;
			  case SelCond::NE:
				if (diff == 0) goto next_tuple;
				break;
			  case SelCond::GT:
				if (diff <= 0) goto next_tuple;
				break;
			  case SelCond::LT:
				if (diff >= 0) goto next_tuple;
				break;
			  case SelCond::GE:
				if (diff < 0) goto next_tuple;
				break;
			  case SelCond::LE:
				if (diff > 0) goto next_tuple;
				break;
		  }
		}

		// the condition is met for the tuple. 
		// increase matching tuple counter
		count++;
		//cout<<count<<endl;

		// print the tuple 
		switch (attr) 
		{
			case 1:  // SELECT key
			  fprintf(stdout, "%d\n", key);
			  break;
			case 2:  // SELECT value
			  fprintf(stdout, "%s\n", value.c_str());
			  break;
			case 3:  // SELECT *
			  fprintf(stdout, "%d '%s'\n", key, value.c_str());
			  break;
		}

		// move to the next tuple
		next_tuple:
		++rid;
	  }
  }

  // If the above "if" wasn't executed, we need to run the indexTree.

  else
  {
  	// cout<<"Need to traverse indexTree"<<endl;
	rid.pid = rid.sid = 0;
	indexFlag = true; // flag that indexfile is open and needs to be closed
	
	// get cursor initial position using equal to and minVal constraints
	if(Eflag) tree.locate(equalVal, cur); // Eflag = 1 implies equality condition on key
	else if(minVal!=-1 && !GEflag) tree.locate(minVal+1, cur); // key > minVal constraint
	else if(minVal!=-1 && GEflag) tree.locate(minVal, cur); // key >= minVal constraint
	else tree.locate(0, cur);
	//cout<<"Cursor PID = "<<cur.pid<<endl;
	//cout<<tree.readForward(cur, key, rid)<<endl;

	/* For normal select, entire table is scanned. However, using index, we start from the
	   found cursor position and evaluate all conditions for each tuple encountered.*/

	while(tree.readForward(cur, key, rid)==0)
	{
		//cout<<"Inside while"<<endl;
		//cout<<attr<<endl;
		if(!valueCondFlag && attr==4) // only for speed up of count(*)
		{
			if(Eflag && key!=equalVal) // if equalVal doesn't match, end
				goto condition_unmet;
			
			if(maxVal!=-1) // if a condition on LT or LE fails, end
			{
				if(LEflag && key>maxVal) goto condition_unmet;
				else if(!LEflag && key>=maxVal) goto condition_unmet;
			}
			
			if(minVal!=-1) // if a condition of GT or GE fails, end
			{
				if(GEflag && key<minVal) goto condition_unmet;
				else if(!GEflag && key<=minVal) goto condition_unmet;
			}
			
			count++;
			//cout<<count<<endl;
			continue;
		}
	
		// read the tuple
		if ((rc = rf.read(rid, key, value)) < 0) {
		  fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
		  goto exit_select;
		}

		// check the conditions on the read tuple
		for (unsigned i = 0; i < cond.size(); i++)
		{
			// compute the difference between the tuple value and the condition value
			switch (cond[i].attr)
			{
				//if condition on key
				case 1:
					diff = key - atoi(cond[i].value);
					break;
				//if condition on value
				case 2:
					diff = strcmp(value.c_str(), cond[i].value);
					break;
			}

			switch (cond[i].comp)
			{
				case SelCond::EQ:
					if (diff != 0)
					{
						if(cond[i].attr==1) goto condition_unmet; // for key, skip all tuples
						else goto next_cursor; // for value, skip this tuple
					}
					break;

				case SelCond::NE:
					if (diff == 0) goto next_cursor; // skip this tuple
					break;

				case SelCond::GT:
					if (diff <= 0) goto next_cursor; // skip this tuple
					break;

				case SelCond::LT:
					if (diff >= 0)
					{
						if(cond[i].attr==1) goto condition_unmet; // for key, skip all tuples
						else goto next_cursor; // for value, skip this tuple
					}
					break;

				case SelCond::GE:
					if (diff < 0) goto next_cursor; // skip this tuple
					break;

				case SelCond::LE:
					if (diff > 0)
					{
						if(cond[i].attr==1) goto condition_unmet; // for key, skip all tuples
						else goto next_cursor; // for value, skip this tuple
					}
					break;
			}
		}

		// the condition is met for the tuple. 
		// increase matching tuple counter
		count++;
		// print the tuple 
		switch (attr)
		{
			case 1:  // SELECT key
			  fprintf(stdout, "%d\n", key);
			  break;
			case 2:  // SELECT value
			  fprintf(stdout, "%s\n", value.c_str());
			  break;
			case 3:  // SELECT *
			  fprintf(stdout, "%d '%s'\n", key, value.c_str());
			  break;
		}
		
		next_cursor:
		cout << ""; //do nothing; we use this to jump to the next while cycle
	}
  }
  
  condition_unmet: // exit early if a condition on a tuple is unmet
  
  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  exit_select:
  if(indexFlag) tree.close(); // indexFlag indicates file was used
	
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
RecordFile rf;
RecordId rid;
RC rc; // RC is integer type

int key; // key - integer type   
string value; // value - string type
string tuple; // for storing each line of input

BTreeIndex btree;

ifstream myfile; // open file in read mode
myfile.open(loadfile.c_str()); // convert to c_str due to ifstream arguments
if(myfile.is_open()) // check if the given file could be successfully opened
{
   rc = rf.open(table + ".tbl", 'w'); // if already present append, else create new

   // insert the index condition here
   // If index is true, append entry and insert (key, RecordId) it into btree
   // else simply append the entry.
int cnt = 0;
   if(index==true)
   {
   		btree.open(table + ".idx", 'w');
   		//cout<<index<<endl; all good
   		while( getline(myfile, tuple) ) // read till the end of file 
 	  	{
    	  parseLoadLine(tuple, key, value); // extract key and value from tuple
      	  
      	  rc = rf.append(key, value, rid); // append to rf

      	  //cout<<rc<<endl; all good

      	  btree.insert(key, rid); // insert into btree
      	  //cnt++;
      	  //cout<<cnt<<endl;
      	  //cout<<"ERROR CODE: "<<rc<<endl;
   		}

   		btree.close();
   }

   else
   {
   		while( getline(myfile, tuple) ) // read till the end of file 
 	  	{
    	  parseLoadLine(tuple, key, value); // extract key and value from tuple
      	  rc = rf.append(key, value, rid); // append to rf
   		}
   }
   rf.close(); // close rf
   myfile.close(); // close myfile
}
  else fprintf(stderr, "Error: Cannot open file."); // error if file could not be opened successfully
  return rc; // return result of opening the RecordFile rf
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
