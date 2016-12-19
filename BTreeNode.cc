#include "BTreeNode.h"
#include <iostream>
#include <cstring>
#include <stdlib.h>	
#include <math.h>

using namespace std;

/*
 * Constructor for Leaf Nodes
 * Clear buffer - set everything to 0
 */

BTLeafNode::BTLeafNode()
{
	fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	/* read function in PageFile loads the disk page with given pid into memory buffer */
	/* buffer is declared as char array of size = PAGE_SIZE (1024 bytes) */
	/* 1 Page = 1 Node */
	return pf.read(pid,buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	/* same as read - just that it loads memory buffer into disk page now*/
	return pf.write(pid,buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int keyCount = 0;
	char* temp = buffer;
	const int groupSize = sizeof(int) + sizeof(RecordId); // 4+(4+4) = 12 bytes
	int limit = PageFile::PAGE_SIZE - sizeof(PageId) - groupSize;
	//int maxKeys = (PageFile::PAGE_SIZE - sizeof(PageId))/groupSize;
	for(int i=0;i<=limit;i=i+groupSize)
	{
		int storedKey;
		memcpy(&storedKey,temp,sizeof(int));
		if(storedKey==0) break;
		else keyCount++;
		temp = temp + groupSize;
	}
	return keyCount;
	
/*  Note: We can also maintain a variable to store this information about number of keys.
	With each insertion, we increment this variable and simply return it in this function.
*/

}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	const int groupSize = sizeof(int) + sizeof(RecordId); // 4+(4+4) = 12 bytes
	int maxKeys = (PageFile::PAGE_SIZE - sizeof(PageId))/groupSize;
	int totalKeys = getKeyCount();
	PageId nextpointer = getNextNodePtr();	
	int limit = PageFile::PAGE_SIZE - sizeof(PageId) - groupSize;
	char* temp=buffer;
	if(totalKeys!=maxKeys)
	{
		int storedKey,i=0;
		for(i=0; i<limit; i= i+groupSize)
		{
			memcpy(&storedKey, temp, sizeof(int));
			if(storedKey==0 || key<=storedKey) break;
			temp = temp + groupSize;
		}
		char* temp1 = (char*)malloc(PageFile::PAGE_SIZE);
		fill(temp1, temp1 + PageFile::PAGE_SIZE, 0); //clear temp1

		memcpy(temp1, buffer, i);
		//transfer first i keygroups in temp to temp1
		memcpy(temp1 + i, &key, sizeof(int)); 
		//transfer key to temp1 after i keygroups
		memcpy(temp1 + i + sizeof(int), &rid, sizeof(RecordId)); 
		//transfer rid to temp1

		memcpy(temp1+groupSize+i, buffer+i, totalKeys*groupSize - i);

		memcpy(temp1+PageFile::PAGE_SIZE-sizeof(PageId), &nextpointer, sizeof(PageId));

		// transfer everything else which was not transferred (including pageid of sibling)

		memcpy(buffer,temp1,PageFile::PAGE_SIZE); 

		// Buffer updated
		
		free(temp1);
		//free(temp);

		return 0;
	}
	else return RC_NODE_FULL;

}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, BTLeafNode& sibling, int& siblingKey)
{ 
	if(sibling.getKeyCount()>0) 
		{
			//cout<<"Invalid here /n";
			return RC_INVALID_ATTRIBUTE;
		}
	// check that the sibling is empty

	const int groupSize = sizeof(int) + sizeof(RecordId); // 4+(4+4) = 12 bytes
	int maxKeys = (PageFile::PAGE_SIZE - sizeof(PageId))/groupSize;
	int totalKeys=0;

	if((totalKeys=getKeyCount()+1)<=maxKeys) return RC_NODE_FULL;
	// split only if this node cannot accommodate one more group

	// start splitting now - divide the keys into half
	int temptotalkeys = getKeyCount();
	int firstHalf = ceil(temptotalkeys/2.0);
	// get half keys

	memcpy(sibling.buffer, buffer+firstHalf*groupSize, PageFile::PAGE_SIZE-sizeof(PageId)-firstHalf*groupSize);
	// store the remaining half keys to sibling's buffer

	sibling.setNextNodePtr(getNextNodePtr());
	// sibling's sibling updated to be the next node of current node

	memcpy(&siblingKey, sibling.buffer, sizeof(int));
	// update siblingKey as first key of sibling node

	fill(buffer+firstHalf*groupSize, buffer + PageFile::PAGE_SIZE - sizeof(PageId), 0);
	// prepare buffer of current node by clearing out other keys and pid info

	int tempKey;
	memcpy(&tempKey, sibling.buffer, sizeof(int));
	// get first key of sibling to check if the keygroup goes into this node or not

	if(key<tempKey) // keygroup goes into current buffer
	{
		insert(key, rid);
	}
	else // keygroup goes into sibling buffer
	{
		sibling.insert(key, rid);
		memcpy(&siblingKey, sibling.buffer, sizeof(int)); //needs to be updated again
	}
	
	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	const int groupSize = sizeof(int) + sizeof(RecordId); // 4+(4+4) = 12 bytes
	char* temp = buffer;
	int storedKey;
	int tempkeys = getKeyCount();
	//cout<<"Value of tempkeys = "<<tempkeys<<;
	for(int i=0; i<tempkeys*groupSize; i=i+groupSize)
	{
		memcpy(&storedKey, temp, sizeof(int));
		if(storedKey==searchKey)
		{
			eid = i/groupSize;
			return 0;
		}
		if(storedKey > searchKey)
		{
			eid = i/groupSize;
			return 0;
		}
		
		temp = temp + groupSize;
	}
	
	eid = tempkeys; // if all keys scanned and yet not found
	return 0;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	const int groupSize = sizeof(int) + sizeof(RecordId); // 4+(4+4) = 12 bytes
	int tempkeys = getKeyCount();
	if(eid < 0 || eid >= tempkeys) return RC_INVALID_CURSOR; 
	else
	{
		char* temp = buffer;
		int index = eid*groupSize;
		memcpy(&key, temp+index, sizeof(int));
		memcpy(&rid, temp+index+sizeof(int), sizeof(RecordId));
		return 0; 
	}
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	char* temp = buffer;
	PageId pid;
	int pidsize = sizeof(PageId);
	memcpy(&pid, temp+PageFile::PAGE_SIZE-pidsize, pidsize);
	//cout<<"Pid returned = "<<pid;
	return pid; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	if(pid < 0)	return RC_INVALID_PID;
	char* temp = buffer;
	int pidsize = sizeof(PageId);
	memcpy(temp+PageFile::PAGE_SIZE-pidsize, &pid, pidsize);
	return 0; 
}

// print function for testing
void BTLeafNode::printLeaf()
{
	const int groupSize = 12;
	int storedKey;
	char* temp = buffer;
	int tempkeys = getKeyCount();
	for(int i=0; i<tempkeys*groupSize; i=i+groupSize)
	{
		memcpy(&storedKey, temp, sizeof(int));
		
		cout << storedKey << " -> ";
		
		temp = temp + groupSize;
	}
	
	//cout << "" << endl;
}




/*
 * Constructor for Non-Leaf Nodes
 * Clear buffer - set buffer to 0
 */

BTNonLeafNode::BTNonLeafNode()
{
	fill(buffer, buffer + PageFile::PAGE_SIZE, 0);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */

/* Node format:	|PageId|(blank 4 bytes)|Key|PageId|Key|PageId|.....|Key|PageId|
*/

int BTNonLeafNode::getKeyCount()
{ 
	int keyCount = 0;
	char* temp = buffer+8; //skip first 8 bytes (pid + empty)
	const int groupSize = sizeof(int) + sizeof(PageId); // 4+4 = 8 bytes
	int storedKey;
	//int maxKeys = (PageFile::PAGE_SIZE - sizeof(PageId))/groupSize;
	for(int i=groupSize; i<=PageFile::PAGE_SIZE - groupSize; i=i+groupSize) //8, 1016
	{
		memcpy(&storedKey, temp, sizeof(int));
		if(storedKey!=0) keyCount++;
		else break;
		temp = temp + groupSize;
	}
	
	return keyCount; 
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	int groupSize = sizeof(int) + sizeof(PageId); //8 bytes
	int maxKeys = (PageFile::PAGE_SIZE - sizeof(PageId))/groupSize;
	int limit = PageFile::PAGE_SIZE - groupSize;
	int tempkeys = getKeyCount();
	//cout<<"Total Keys here = "<<tempkeys<<endl;
	if(tempkeys==maxKeys) return RC_NODE_FULL;
	else
		{
			char* temp = buffer + groupSize;
			int storedKey, i=8; // we know i should be 8
			for(i=groupSize; i<=limit; i=i+groupSize)
			{
				memcpy(&storedKey, temp, sizeof(int));
				if(storedKey==0) break;
				if(key<=storedKey) break;
				temp = temp + groupSize;
			}
			char* temp1 = (char*)malloc(PageFile::PAGE_SIZE);
			fill(temp1, temp1 + PageFile::PAGE_SIZE, 0); //clear temp1
			memcpy(temp1, buffer, i);
			memcpy(temp1+i, &key, sizeof(int));
			memcpy(temp1+i+sizeof(int), &pid, sizeof(PageId));
			memcpy(temp1+groupSize+i, buffer+i, (tempkeys+1)*groupSize - i);
			memcpy(buffer, temp1, PageFile::PAGE_SIZE);
		}

	return 0; 
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	/* 
	Node Format:
	
	|PageId|Blank(4 bytes)|Key|PageId|...|Key|PageId| */

	// Check that the sibling node is empty
	if(sibling.getKeyCount()!=0) return RC_INVALID_ATTRIBUTE;

	int groupSize = sizeof(PageId) + sizeof(int);
	int maxKeys = (PageFile::PAGE_SIZE-sizeof(PageId))/groupSize;
	int tempkeys = getKeyCount();
	
	// Check if we need to actually split (i.e. check if insertion leads to overflow)
	if(!(tempkeys >= maxKeys)) return RC_INVALID_FILE_FORMAT;
	
	// Clear sibling buffer
	fill(sibling.buffer, sibling.buffer + PageFile::PAGE_SIZE, 0);

	// Calculate keys to remain in the first half
	int numHalfKeys = ceil(tempkeys/2.0);
	int halfIndex = numHalfKeys*groupSize + groupSize; // initial offset of 8
	
	//REMOVING THE MEDIAN KEY
	
	int keyFH, keySH;
	memcpy(&keyFH, buffer+halfIndex-groupSize, sizeof(int)); // last key of fh
	memcpy(&keySH, buffer+halfIndex, sizeof(int)); // first key of sh
	
	if(key > keySH) // then keySH = median
	{
		memcpy(sibling.buffer+8, buffer+halfIndex+8, PageFile::PAGE_SIZE-halfIndex-8);
		memcpy(&midKey, buffer+halfIndex, sizeof(int));
		memcpy(sibling.buffer, buffer+halfIndex+4, sizeof(PageId));
		fill(buffer+halfIndex, buffer + PageFile::PAGE_SIZE, 0);

		// Insert new key and pid into buffer of sibling's node 
		sibling.insert(key, pid);
		
	}

	else if(key < keyFH) // then keyFH = median
	{
		memcpy(sibling.buffer+groupSize, buffer+halfIndex, PageFile::PAGE_SIZE-halfIndex);
		memcpy(&midKey, buffer+halfIndex-8, sizeof(int));
		memcpy(sibling.buffer, buffer+halfIndex-4, sizeof(PageId));
		fill(buffer+halfIndex-groupSize, buffer + PageFile::PAGE_SIZE, 0); 
		
		// Insert new key and pid into buffer of current node
		insert(key, pid);		
	}
	
	else // then key = median
	{
		memcpy(sibling.buffer+groupSize, buffer+halfIndex, PageFile::PAGE_SIZE-halfIndex);
		fill(buffer+halfIndex, buffer + PageFile::PAGE_SIZE, 0); 
		midKey = key;
		memcpy(sibling.buffer, &pid, sizeof(PageId));

		// no key insertion here.
	}
	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	int groupSize = sizeof(int) + sizeof(PageId);
	char* temp = buffer + groupSize;
	int storedKey;
	bool check = true;

	/* Node format:
	|PageId|(blank 4 bytes)|Key|PageId|Key|PageId|.....|Key|PageId|
	*/
	int tempkeys = getKeyCount();
	for(int i=groupSize; i<(tempkeys+1)*groupSize; i=i+groupSize)
	{
		check = (i==groupSize);
		memcpy(&storedKey, temp, sizeof(int));
		if(storedKey > searchKey && check)
		{
			memcpy(&pid, buffer, sizeof(PageId));
			return 0;
		}
		else if(storedKey>searchKey)
		{
			memcpy(&pid, temp-sizeof(PageId), sizeof(PageId));
			return 0;
		}
		//Keep checking till searchKey is less than storedKey
		temp = temp + groupSize; 
		
	}
	
	// This implies that the SearchKey is greater than all keys
	memcpy(&pid, temp-sizeof(PageId), sizeof(PageId));
	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	fill(buffer, buffer + PageFile::PAGE_SIZE, 0); // set buffer to zero
	char* temp = buffer;
	int psize = sizeof(PageId);
	memcpy(temp, &pid1, psize); //set pid of temp;
	RC err = insert(key, pid2);
	return err;
}
