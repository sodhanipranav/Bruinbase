/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstdio>
#include <iostream>
#include <cstring>
#include <stdlib.h>
#include <math.h>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0; // initialize height of the B+ Tree to 0.
    fill(buffer, buffer + PageFile::PAGE_SIZE, 0); //set buffer entries to zero.
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    //cout<<"Tree Height is: "<<treeHeight<<endl;
    RC error;
    
    error = pf.open(indexname, mode);
    if(error==0) {/*cout<<"OK so far... \n"*/;}
    else return error;
    
    error = pf.read(0,buffer);
    if(error==0) {/*cout<<"OK so far.... \n"*/;}
    else return error;

    int bufferPid;
    memcpy(&bufferPid, buffer, sizeof(PageId));
    if(bufferPid>=1) rootPid = bufferPid;

    int bufferTreeHeight;
    memcpy(&bufferTreeHeight, buffer+sizeof(PageId), sizeof(int));
    if(bufferTreeHeight>=1) treeHeight = bufferTreeHeight;

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	 //cout<<"Tree Height is: "<<treeHeight<<endl;
	// copy rootPid and treeHeight to disk before closing.
	// As mentioned in the project description, these variables need to be saved or else
	// they get deleted from memory

	// copy to memory
    memcpy(buffer, &rootPid, sizeof(PageId));
	memcpy(buffer+sizeof(PageId), &treeHeight, sizeof(int));
	
	// write to disk
	RC error;
	error = pf.write(0, buffer);
	if(error==0) {/*cout<<"OK so far.... \n"*/;}
	else return error;

	// close file now
	error = pf.close();
	if(error!=0) return error;
    else return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    // 2 cases: treeHeight = 0 and treeHeight!=0

    if(treeHeight==0)
    {
    	//cout<<"Tree Height is zero."<<endl;
    	RC error;
    	BTLeafNode leafNode;
    	leafNode.insert(key,rid);
    	treeHeight++;

    	if(pf.endPid()==0)  rootPid = 1;
		else  rootPid = pf.endPid();

		error = leafNode.write(rootPid, pf);
		return error;
    }
    else
    {
    	//cout<<"Tree Height is not zero."<<endl;
    	RC error;
    	PageId insertPid = -1;
    	int midKey = -1;
    	int height = 1;
    	error = helper_insert(key,rid,rootPid,height,midKey,insertPid);
    	return error;
    }
}
RC BTreeIndex::helper_insert(int key, const RecordId& rid, PageId pagePid, int height, int& midKey, PageId& insertPid)
{
	RC error;
	
	midKey = -1;
	insertPid = -1;
	
	if(height==treeHeight)
	{
		//cout<<"Height==TreeHeight"<<endl;
		BTLeafNode leafNode;
		leafNode.read(pagePid, pf);

		//Return if the leaf node gets inserted successfully
		if(leafNode.insert(key, rid)==0)
		{	
			leafNode.write(pagePid, pf);
			return 0;
		}

		int otherKey;
		BTLeafNode otherLeafNode;
		error = leafNode.insertAndSplit(key, rid, otherLeafNode, otherKey);
		
		if(error==0) {/*cout<<"OK so far... \n"*/;}
    	else return error;
		
		int lastPid = pf.endPid();
		midKey = otherKey;
		insertPid = lastPid;

		//Update leafNode and otherLeafNode
		otherLeafNode.setNextNodePtr(leafNode.getNextNodePtr());
		leafNode.setNextNodePtr(lastPid);

		error = otherLeafNode.write(lastPid, pf);
		
		if(error==0) {/*cout<<"OK so far... \n"*/;}
    	else return error;
		
		error = leafNode.write(pagePid, pf);
		
		if(error==0) {/*cout<<"OK so far... \n"*/;}
    	else return error;
		
		if(treeHeight==1)
		{
			//Create a new Root if there was only one node in the beginning
			BTNonLeafNode newRoot;
			newRoot.initializeRoot(pagePid, otherKey, lastPid);
			treeHeight++;
			
			//Update rootPid
			rootPid = pf.endPid();
			newRoot.write(rootPid, pf);
		}
		
		return 0;
	}
	else
	{
		//cout<<"Height NOT=TreeHeight"<<endl;
		//Locate the node where the new key should be inserted
		BTNonLeafNode midNode;
		midNode.read(pagePid, pf);
		
		PageId childPid = -1;
		midNode.locateChildPtr(key, childPid);
		
		int insertKey = -1;
		PageId insertPid = -1;
		
		error = helper_insert(key, rid, childPid, height+1, insertKey, insertPid);
		
		//Error might occur if node was full
		if(!(insertKey==-1 && insertPid==-1)) 
		{
			RC error2 = midNode.insert(insertKey, insertPid);
			if(error2==0)
			{
				midNode.write(pagePid, pf);
				return 0;
			}
			//else, try insertAndSplit again
			BTNonLeafNode anotherMidNode;
			int otherKey;
			
			midNode.insertAndSplit(insertKey, insertPid, anotherMidNode, otherKey);
			
			int lastPid = pf.endPid();
			midKey = otherKey;
			insertPid = lastPid;
			
			//Update midNode and anotherMidNode
			error = midNode.write(pagePid, pf);
			if(error==0) {/*cout<<"OK so far... \n"*/;}
	    	else return error;
			
			error = anotherMidNode.write(lastPid, pf);
			if(error==0) {/*cout<<"OK so far... \n"*/;}
    		else return error;
			
			if(treeHeight==1)
			{
				BTNonLeafNode newRoot;
				newRoot.initializeRoot(pagePid, otherKey, lastPid);
				treeHeight++;		
				rootPid = pf.endPid();
				newRoot.write(rootPid, pf);
			}
		}
		return 0;
	}
}
/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    RC error;
    int eid;
    //int height =1;
    if (treeHeight<=0)
    {
        cout <<"No nodes can be located.Aborting. \n";
        return RC_NO_SUCH_RECORD;
    }

    BTNonLeafNode NonLeaf;
    PageId nextPid=rootPid;
    for(int height = 1; height!=treeHeight;)
    {
        error = NonLeaf.read(nextPid, pf);
       // cout<<"Non Leaf Read Error "<<error<<endl;
        if(error!=0) return error;

        error = NonLeaf.locateChildPtr(searchKey, nextPid);
       // cout<<"Non Leaf Child Locate Error "<<error<<endl;
        if(error!=0) return error;

        height++;
    }
    BTLeafNode leafNode;

    error = leafNode.read(nextPid, pf);
    //cout<<"Leaf Read Error "<<error<<endl;
    if(error!=0) return error;

    error = leafNode.locate(searchKey, eid);
    //cout<<"Leaf Read Error "<<error<<endl;
    if(error!=0) return error;

    // Update IndexCursor now.
    cursor.pid = nextPid;
    cursor.eid = eid;

    return error;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    //IndexCursor Details:
	int cEid = cursor.eid;
	PageId cPid = cursor.pid;
		
	//Cursor's leaf loaded using cPid
	BTLeafNode leafNode;
	RC error = leafNode.read(cPid, pf);
	if(error==0) {/*cout<<"OK so far .. \n"*/;}
	else return error;
	//Incase the cursor pid is not valid, return error = RC_NO_SUCH_RECORD

	//Find and return the key and RecordId using cursor eid (cEid)
	error = leafNode.readEntry(cEid, key, rid);
	if(error==0) {/*cout<<"OK so far .. \n"*/;}
	else return error;
	//cout<<"CPiD: "<<cPid<<endl;
	if(cPid <= 0) return RC_NO_SUCH_RECORD;

	//Increment cEid and check it is less than max
	int count = leafNode.getKeyCount();
	//If not exceeded,
	if(cEid+1 < count)
	{
		cEid++; //simply increment cursor eid
	}
	//if exceeded,
	else
	{
		cEid = 0; //reset cEid
		cPid = leafNode.getNextNodePtr(); //update cPid
	}
	
	//update new eid and pid to cursor parameters.
	cursor.eid = cEid;
	cursor.pid = cPid;
	return 0;
}
