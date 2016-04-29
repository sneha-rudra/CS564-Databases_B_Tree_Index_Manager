/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <limits.h>
#include "btree.h"
#include "filescan.h"
#include <file_iterator.h>
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_not_pinned_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
BTreeIndex::BTreeIndex(const std::string & relationName, std::string & outIndexName, BufMgr *bufMgrIn, const int attrByteOffset, const Datatype attrType) {

	//TODO: UNPIN UNNEEDED PAGES THROUGH OUT THIS FUNCTION

    //create the filename
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();
	
	//set values of the private variables
	bufMgr = bufMgrIn;
    attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
	scanExecuting = false;
	headerPageNum = 1;

	if(attrType == INTEGER) {
		leafOccupancy = INTARRAYLEAFSIZE;
		nodeOccupancy = INTARRAYNONLEAFSIZE;
	} else if(attrType == DOUBLE) {
		leafOccupancy = DOUBLEARRAYLEAFSIZE;
		nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
	} else if(attrType == STRING) {
		leafOccupancy = STRINGARRAYLEAFSIZE;
		nodeOccupancy = STRINGARRAYNONLEAFSIZE;
	} else {
		std::cout << "ERROR: non valid data type passed to BTreeIndex constructor" << std::endl;
		return;
	}

	//std::cout << "nodeOccupancy = " << nodeOccupancy << std::endl;
	//std::cout << "leafOccupancy = " << leafOccupancy << std::endl;


    //check if that file exists
	Page* rootPage;
	Page* metadataPage;
	IndexMetaInfo* metadata;
	BlobFile* bFile;
    try {
		bFile = new BlobFile(outIndexName, true);
	} catch(const FileExistsException &e) {

		//std::cout << "File already existed" << std::endl;

		bFile = new BlobFile(outIndexName, false);
		file = (File*) bFile;

		//if the file exists, read the first page which contains metadata information
		bufMgr->readPage(file, headerPageNum, metadataPage);
		metadata = (IndexMetaInfo*) metadataPage;

		//make sure the metadata matches whats passed in if the file already exists
		if(metadata->attrType != attrType ||
			metadata->attrByteOffset != attrByteOffset ||
			strcmp(metadata->relationName, relationName.c_str()) != 0) {

			//if something doesnt match, then throw an exception
			throw BadIndexInfoException("Info passed into constructor doesn't match meta info page");
		}

		//we dont need the header information anymore and we didnt change anything on that page
		bufMgr->unPinPage(file, headerPageNum, false);

		//set the root page for this index
		rootPageNum = metadata->rootPageNo;

		//we are going to keep the rootPage in memory 
		bufMgr->readPage(file, rootPageNum, rootPage);
		return;
	}
	
	//std::cout << "Creating a new file for this index" << std::endl;

	//if the code reaches here then the file didnt exist but we created one
	file = (File*) bFile;
	
	//make a metadata Page for this new index, should be the first page
	PageId metadataPageId;
	bufMgr->allocPage(file, metadataPageId, metadataPage);
	headerPageNum = metadataPageId; //just in case it isnt 1 and we need to get at it later save where it is
	bufMgr->readPage(file, metadataPageId, metadataPage);
	metadata = (IndexMetaInfo*) metadataPage;
	
	//set variables in the metadata page
	strncpy(metadata->relationName, relationName.c_str(), 20);
	metadata->attrType = attrType;
	metadata->attrByteOffset = attrByteOffset;

	//create a new root page
	bufMgr->allocPage(file, rootPageNum, rootPage);
	bufMgr->readPage(file, rootPageNum, rootPage);
	metadata->rootPageNo = rootPageNum;

	//std::cout << "rootPageNum = " << rootPageNum << std::endl;

	//now we can unpin the metaPage. Its dirty and needs to be written to disk
	bufMgr->unPinPage(file, metadataPageId, true);
	
	//the rootPage will become a non-leaf node
	switch(attrType) {
		case Datatype::INTEGER: {
			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++) rootNode->keyArray[i] = INT_MAX;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty left leaf page and right leaf page of the attribute type
			Page* leftLeafPage, *rightLeafPage; 
			PageId leftLeafPageId;
			PageId rightLeafPageId;

			bufMgr->allocPage(file, leftLeafPageId, leftLeafPage);
			bufMgr->allocPage(file, rightLeafPageId, rightLeafPage);
			
			bufMgr->readPage(file, leftLeafPageId, leftLeafPage);
			bufMgr->readPage(file, rightLeafPageId, rightLeafPage);

			LeafNodeInt* leftLeafNode = (LeafNodeInt*) leftLeafPage;
			LeafNodeInt* rightLeafNode = (LeafNodeInt*) rightLeafPage;
			
			//initialize the leaf page
			for(int i = 0; i < leafOccupancy; i++) {
				leftLeafNode->keyArray[i] = INT_MAX;
				rightLeafNode->keyArray[i] = INT_MAX;
			}

			leftLeafNode->rightSibPageNo = rightLeafPageId;
			rightLeafNode->rightSibPageNo = NULL;

			rootNode->pageNoArray[0] = leftLeafPageId;
			rootNode->pageNoArray[1] = rightLeafPageId;

			//unpin the new leaf page. its dirty
			bufMgr->unPinPage(file, leftLeafPageId, true);
			bufMgr->unPinPage(file, rightLeafPageId, true);
			break;
		}
		case Datatype::DOUBLE: {

			//TODO: follow template from int. things have changed in there and havent made it to each double and string yet

			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++) rootNode->keyArray[i] = NULL;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty leaf page of the attribute type
			Page* leafPage; PageId leafPageId;
			bufMgr->allocPage(file, leafPageId, leafPage);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeDouble* leafNode = (LeafNodeDouble*) leafPage;

			//initialize the leaf page
			leafNode->rightSibPageNo = NULL;
			for(int i = 0; i < leafOccupancy; i++) leafNode->keyArray[i] = NULL;
			for(int i = 0; i < leafOccupancy; i++) {
				RecordId rid;
				rid.page_number = NULL;
				rid.slot_number = NULL;
				leafNode->ridArray[i] = rid;
			}

			rootNode->pageNoArray[0] = leafPageId;
			break;
		}
		case Datatype::STRING: {

			//TODO: follow template from int. things have changed in there and havent made it to each double and string yet

			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeString* rootNode = (NonLeafNodeString*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++)
				for(int j = 0; j < STRINGSIZE; j++)
					rootNode->keyArray[i][j] = NULL;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty leaf page of the attribute type
			Page* leafPage; PageId leafPageId;
			bufMgr->allocPage(file, leafPageId, leafPage);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeString* leafNode = (LeafNodeString*) leafPage;

			//initialize the leaf page
			leafNode->rightSibPageNo = NULL;
			for(int i = 0; i < leafOccupancy; i++)
				for(int j = 0; j < STRINGSIZE; j++)
					leafNode->keyArray[i][j] = NULL;
			for(int i = 0; i < leafOccupancy; i++) {
				RecordId rid;
				rid.page_number = NULL;
				rid.slot_number = NULL;
				leafNode->ridArray[i] = rid;
			}

			rootNode->pageNoArray[0] = leafPageId;
			break;
		}
		default: { break; }
	}

	

	//insert records from this relation into the tree
	//Create a file scanner for this relaion and buffer manager
	FileScan* fileScan = new FileScan(relationName, bufMgr);
	RecordId rid;
	const char* recordPtr;
	std::string record;
	try {
		//when we reach the end of this file, an exception will be thrown so we will exit then
		while(true) {
			fileScan->scanNext(rid);
			record = fileScan->getRecord();
			recordPtr = record.c_str();
			std::string key;

			switch(attrType) {
				case INTEGER:
					//std::cout << *((int*) (recordPtr + attrByteOffset)) << std::endl;
					//std::cout << "\t" << rid.page_number << " " << rid.slot_number << std::endl;
					insertEntry((void*) (recordPtr + attrByteOffset), rid);
					break;
				case DOUBLE:
					insertEntry((void*) (recordPtr + attrByteOffset), rid);
					break;
				case STRING: 
					char keyBuf[STRINGSIZE];
					strncpy(keyBuf, (char*)(recordPtr + attrByteOffset), sizeof(keyBuf));
					key = std::string(keyBuf);
					insertEntry((void*) &key, rid);
					break;
				default: { break; }
			}
		}
	} catch (EndOfFileException &e) {
		//end of the scan has been reached
	}
	
	/*
	int i;
	for(i = 0; i < nodeOccupancy; i++) {
		if(((NonLeafNodeInt*) rootPage)->keyArray[i] != INT_MAX) {
			Page* page;
			bufMgr->readPage(file, ((NonLeafNodeInt*) rootPage)->pageNoArray[i], page);
			LeafNodeInt* leaf = (LeafNodeInt*) page;
			std::cout << "(" << ((NonLeafNodeInt*) rootPage)->pageNoArray[i] << ", " << ((NonLeafNodeInt*) rootPage)->keyArray[i] << ", " << leaf->rightSibPageNo <<") ";
		}
		else break;
	}

	Page* mypage;
	bufMgr->readPage(file, 4, mypage);
	LeafNodeInt* mynode = (LeafNodeInt*) mypage;
	std::cout << "@@@@@@@@@PAGE4" << std::endl;
	for(int j = 0; j < leafOccupancy; j++) {
		if(mynode->keyArray[j] != INT_MAX) std::cout << j << ":" << mynode->keyArray[j] << " ~ ";
	}
	std::cout << " " << std::endl;

	bufMgr->readPage(file, 5, mypage);
	mynode = (LeafNodeInt*) mypage;
	std::cout << "@@@@@@@@@PAGE5" << std::endl;
	for(int j = 0; j < leafOccupancy; j++) {
		if(mynode->keyArray[j] != INT_MAX) std::cout << j << ":" << mynode->keyArray[j] << " ~ ";
	}
	std::cout << std::endl;
		
	//std::cout << ((NonLeafNodeInt*) rootPage)->pageNoArray[i] << std::endl;
	*/
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
BTreeIndex::~BTreeIndex()
{
	// Destructor. Method does not throw any exceptions as is indicated in the header file. All exceptions are caught in here itself.
	
	// TODO: Performing cleanup by clearing up state variables
	
	// Ending any initialized scan  and unpinning any B+ Tree pages that are pinned by invoking the endScan method.
	// endScan method can throw the ScanNotInitializedException and PageNotPinned (thrown by unPinPage) which are caught in here
	if(scanExecuting)
	{
		try {
			endScan();
		} catch(const ScanNotInitializedException &e) { 
			std::cout << "ScanNotInitializedException thrown in BtreeIndex destructor\n"; 
		}
	}
	
	// Flushing the index file from the buffer manager if it exists
	if(file) {
		bufMgr->flushFile(file);
	}
	
	// Deleting the file object instance. This automatically invokes the destructor of the File class and closes the index file.
	delete file;
	
	// TODO: Remember to clean up any state variables. Maybe state variables that we set up in the constructor? 
	// NOTE: The ~FileScan method (which also shuts down scan and unpins any pinned pages) sets the currentPage to null, clears the dirty bit and sets the file iterator 
	// to point to the beginning of the file. Not sure if that's what they mean by perform any cleanup. This is not done in any other version though.
	// NOTE: Printng out error messages for now. We can remove them if required.
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	//read in the root page. Should already be in the buffer
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);

	//cast the rootPage to a non leaf node depending on type
	switch(attributeType) {
		case Datatype::INTEGER: {
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
			bool restructured;
			bool comingFromLeaf;
			PageId newPageId;
			//const void* middleKey;

			traverseAndInsert(rootPage, rootNode->level, true, key, rid, restructured, newPageId, comingFromLeaf);

			//TODO: if the root was restructured then update the metapage!!!
			if(restructured) {
				if(rootNode->keyArray[nodeOccupancy - 1] == INT_MAX) {

					insertIntoNonLeafPage(rootPage, (void*) &middleInt, newPageId);

				} else {
					
					//FIX BELOW
					/*
					restructured = true;

					restructure(rootPage, false, middleKey, newPageId, newPageId, middleKey);

					//only need to insert if 
					if(*((int*) middleKeyFromChild) < *((int*) middleKey)) {
						//insert it onto old node (nodeInt)
						insertIntoNonLeafPage(page, middleKeyFromChild, pageIdFromChild);
					} else if(*((int*) middleKeyFromChild) > *((int*) middleKey)) {
						//insert it onto the new node the child created
						//read in that page
						Page* newNodePage;
						bufMgr->readPage(file, newPageId, newNodePage);

						insertIntoNonLeafPage(newNodePage, middleKeyFromChild, pageIdFromChild);

						//TODO: unpin that page
					}
					*/
				}
			}
			//read in the new root page given the pageId from the function

			//unpin the old root page

			break;
		}
		case Datatype::DOUBLE: {
			break;
		}
		case Datatype::STRING: {
			break;
		}
		default: {};
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
const void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm, const void* highValParm, const Operator highOpParm) {
	if(scanExecuting) {
		return;
	}

	//set the local values for this class to the values passed in
	lowOp = lowOpParm;
	highOp = highOpParm;

	// lowOp must be either GT/GTE and highOp must be LT/LTE. BadOpCodesException is thrown if that is not the case.
	if( !((lowOp == GT)||(lowOp == GTE)) || !((highOp == LT)||(highOp == LTE)) ) {
		throw BadOpcodesException();
	}

	//std::cout << "STARTING TO SCAN" << std::endl;

	//read in the rootPage
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);

	switch(attributeType) {
		case INTEGER: {
			lowValInt = *((int*) lowValParm);
			highValInt = *((int*) highValParm);

			// Method throws exception if lower bound > upper bound
			if(lowValInt > highValInt) {
				throw BadScanrangeException();
			}

			//traverse to get to the leafPageId
			Page* leafPage;
			PageId leafPageId;
			traverse(rootPage, ((NonLeafNodeInt*) rootPage)->level, lowValParm, leafPageId);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeInt* leaf = (LeafNodeInt*) leafPage;

			//find the first record then set the class variables
			bool firstRecordFound = false;
			while(!firstRecordFound) {
				//loop through all the keys on the leaf

				//std::cout << "searching page " << leafPageId << " lowOP " << lowOp << " high OP " << highOp << std::endl;

				for(int i = 0; i < leafOccupancy; i++) {
					//if(leaf->keyArray[i] != INT_MAX) std::cout << "keyArray[" << i << "] = " << leaf->keyArray[i] << std::endl;

					if(leaf->keyArray[i] != INT_MAX && 
						((lowOp == GT && leaf->keyArray[i] > lowValInt) || (lowOp == GTE && leaf->keyArray[i] >= lowValInt)) && 
						((highOp == LT && leaf->keyArray[i] < highValInt) || (highOp == LTE && leaf->keyArray[i] <= highValInt))) {
						currentPageData = leafPage;
						currentPageNum = leafPageId;
						nextEntry = i;

						//std::cout << "Found " << leaf->keyArray[i] << " at index " << i << " on page " << leafPageId << std::endl;
						//for(int j = 0; j < leafOccupancy; j++) {
						//	std::cout << j << ":" << leaf->keyArray[j] << " ~ ";
						//}
						//std::cout << std::endl;
						firstRecordFound = true;
						break;
					} 
				}

				//if the first record still hasnt been found, check the next page
				if(!firstRecordFound) {
					//read in the next page if you can
					if(leaf->rightSibPageNo != NULL) {
						PageId nextPageId = leaf->rightSibPageNo;
						bufMgr->readPage(file, nextPageId, leafPage);

						//unpin the previous one
						bufMgr->unPinPage(file, leafPageId, false);
						
						//set the new pointer to the page that was just read in
						leafPageId = nextPageId;
						leaf = (LeafNodeInt*) leafPage;
					} else {
						//we've reached the end of our data and still havent found anything greater than the lowParm
						nextEntry = -1;
						firstRecordFound = true; //just to break out of the loop
					}
					
				}
			}
			break;
		}
		case DOUBLE: {
		
			break;
		}
		case STRING: {
		
			break;
		}
		default: { break; }
	
	}


	//after we know everything is good to go, then set scanExecuting = true
	scanExecuting = true;
	
	//switch on attrType and cast the void* to appropriate type

	//find the leaf page where the lowValParm would fit in as a key
	
	//read the rootPage into the bufMgr
	//cast that page to a non-leaf node

	// Where to unpin

	//while(node->level == 0) {
	//	loop through the keys in that level to find where lowValParm would go
	//	set node = child pointed to by page (make sure to read the page into memory first)
	//	break from the for loop when you find the correct page
	//}

	//node now equals the level above the correct leaf
	//create a pointer to a leaf page called leaf

	//once again loop through the node (nonLeafNode) 
	//	find where the lowValParm would go and read that page into the bufMgr
	//
	//set leaf to leafNode* cast of the page pointer

	//loop through the keys on the leaf page
	//(unpin page before throw exception)
	//if the first key you find that is bigger than the lower bound and upper bound, then exception
	//or if the lower bound is larger than the last key, then exception

	//


}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(!scanExecuting) throw ScanNotInitializedException();

	if(nextEntry == -1) throw  IndexScanCompletedException();

	//read in the page that we are currently scanning
	bufMgr->readPage(file, currentPageNum, currentPageData);

	switch(attributeType) {
		case INTEGER: {
			LeafNodeInt* leaf = (LeafNodeInt*) currentPageData;
			outRid = leaf->ridArray[nextEntry];

			//std::cout << "currPageNum=" << currentPageNum << " keyArray[" << nextEntry << "] = " << leaf->keyArray[nextEntry] << std::endl;

			if(nextEntry == leafOccupancy - 1 || leaf->keyArray[nextEntry+1] == INT_MAX) {
				//bring in the next page if we can. if there is no next page then set nextEntry to -1 
				if(leaf->rightSibPageNo != NULL) {
					Page* nextPage;
					PageId newPageId = leaf->rightSibPageNo;
					bufMgr->readPage(file, newPageId, nextPage);

					//unpin the previous page
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageData = nextPage;
					currentPageNum = newPageId;

					leaf = (LeafNodeInt*) nextPage;
					if((highOp == LT && leaf->keyArray[0] < highValInt) || (highOp == LTE && leaf->keyArray[0] <= highValInt)) {
						nextEntry = 0;
					} else {
						nextEntry = -1;
					}
				} else {
					nextEntry = -1;
				}
				
			} else {
				if((highOp == LT && leaf->keyArray[nextEntry+1] < highValInt) || (highOp == LTE && leaf->keyArray[nextEntry+1] <= highValInt)) {
					nextEntry++;
				} else {
					nextEntry = -1;
				}
			}

			break;
		}
		case DOUBLE: {
		
			break;
		} 
		case STRING: { 
		
			break;
		}
		default: { break; }
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
const void BTreeIndex::endScan() 
{
	// Method terminates the current scan and  throws a ScanNotInitializedException if invoked before a succesful startScan call
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;

	// Unpinning all the pages that have been pinned for the purpose of scan
	try {
		bufMgr->unPinPage(file, currentPageNum, false);
	} catch(const PageNotPinnedException &e) {
		std::cout << "PageNotPinned thrown in endScan()\n";
	}
	// TODO: Should the dirty bit be set to false irrespective of whether the page is actually dirty or not?
	// TODO: Check if the currentPage is the only page pinned for the purpose of the scan 
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertIntoNonLeafPage
// -----------------------------------------------------------------------------
const void BTreeIndex::insertIntoNonLeafPage(Page* page, const void* keyPtr, PageId pageId) {
	switch(attributeType) {
		case INTEGER: {
			NonLeafNodeInt* node = (NonLeafNodeInt*) page;
			int key = *((int*) keyPtr);

			for(int i = 0; i < nodeOccupancy; i++) {
				if(node->keyArray[i] == INT_MAX) {
					node->keyArray[i] = key;
					node->pageNoArray[i+1] = pageId;
					break;
				} else if(key < node->keyArray[i]) {
					//move everything over to the right
					for(int j = nodeOccupancy - 1; j <= i; j--) {
						node->keyArray[j] = node->keyArray[j-1];
						node->pageNoArray[j+1] = node->pageNoArray[j];
					}
					node->keyArray[i] = key;
					node->pageNoArray[i+1] = pageId;
					break;
				}
			}
			break;
		}
		case DOUBLE: {

			break;
		}
		case STRING: {

			break;
		}
		default: {break; }
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::restructure
// -----------------------------------------------------------------------------
const void BTreeIndex::restructure(Page* fullPage, bool isLeaf, const void* keyPtr, PageId newPageIdFromChild, PageId &newPageId) {
	switch(attributeType) {
		case INTEGER: {
			if(isLeaf) {
				//cast the fullPage to a leaf
				LeafNodeInt* fullLeaf = (LeafNodeInt*) fullPage;

				//create a new page
				Page* newLeafPage;
				bufMgr->allocPage(file, newPageId, newLeafPage);
				bufMgr->readPage(file, newPageId, newLeafPage);
				LeafNodeInt* newLeaf = (LeafNodeInt*) newLeafPage;

				//NULL everything in the new page
				for(int i = 0; i < leafOccupancy; i++) {
					newLeaf->keyArray[i] = INT_MAX;
					//not NULLing the rids here because we just assume if the key is NULL then so is the associated rid so don't access it
				}

				//get the middle value and index from the page
				int middleIndex;
				findMiddleValue(fullPage, true, keyPtr, middleIndex);

				//copy all the keys and rids over from middleIndex
				for(int i = middleIndex; i < leafOccupancy; i++) {
					newLeaf->keyArray[i-middleIndex] = fullLeaf->keyArray[i];
					fullLeaf->keyArray[i] = INT_MAX;
					newLeaf->ridArray[i-middleIndex] = fullLeaf->ridArray[i];
					//not NULLing rids here again, just check if corresponding key is null to see if the data is valid
				}

				newLeaf->rightSibPageNo = fullLeaf->rightSibPageNo;
				fullLeaf->rightSibPageNo = newPageId;
			} else {
				//cast the fullPage to a nonleaf
				NonLeafNodeInt* fullNode = (NonLeafNodeInt*) fullPage;

				//create a new page
				Page* newNodePage;
				bufMgr->allocPage(file, newPageId, newNodePage);
				bufMgr->readPage(file, newPageId, newNodePage);
				NonLeafNodeInt* newNode = (NonLeafNodeInt*) newNodePage;

				//NULL everything in the new page 
				newNode->pageNoArray[nodeOccupancy] = NULL;
				for(int i = 0; i < nodeOccupancy; i++) {
					newNode->keyArray[i] = INT_MAX;
					newNode->pageNoArray[i] = NULL;
				}

				//get the middle value and index from the page
				int middleIndex;
				findMiddleValue(fullPage, false, keyPtr, middleIndex);

				//if the keyPtr we are trying to insert is the middle one the some special stuff happens
				if(*((int*) keyPtr) == middleInt) {
					newNode->pageNoArray[0] = newPageIdFromChild;

					for(int i = middleIndex + 1; i < nodeOccupancy; i++) {
						newNode->keyArray[i-middleIndex-1] = fullNode->keyArray[i];
						fullNode->keyArray[i] = INT_MAX;
						newNode->pageNoArray[i-middleIndex] = fullNode->pageNoArray[i+1];
					}
				} else {
					for(int i = middleIndex + 1; i < nodeOccupancy; i++) {
						newNode->keyArray[i-middleIndex-1] = fullNode->keyArray[i];
						fullNode->keyArray[i] = INT_MAX;
						newNode->pageNoArray[i-middleIndex-1] = fullNode->pageNoArray[i];
					}
					newNode->pageNoArray[nodeOccupancy-middleIndex+1] = fullNode->pageNoArray[nodeOccupancy];
				}
			}
			break;
		}
		case DOUBLE: {
		
			break;
		}
		case STRING: {
		
			break;
		}
		default: { break; }
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::traverseAndInsert
// -----------------------------------------------------------------------------
const void BTreeIndex::traverseAndInsert(Page* page, int pageLevel, bool isRoot, const void* keyPtr, const RecordId rid, bool &restructured, PageId &newPageId, bool &comingFromLeaf) {
	switch(attributeType) {
		case INTEGER: {
			int key = *((int*) keyPtr);
			NonLeafNodeInt* nodeInt = (NonLeafNodeInt*) page;

			if(isRoot && nodeInt->keyArray[0] == INT_MAX) {
				//set the first key in the root 
				nodeInt->keyArray[0] = key;
			}

			if(pageLevel == 0) {
				comingFromLeaf = false;

				//find where the key would go
				int index = findIndexIntoPageNoArray(page, keyPtr);

				//read in that page
				Page* child;
				bufMgr->readPage(file, nodeInt->pageNoArray[index], child);

				//recurse into that child
				PageId pageIdFromChild;
				bool childRestructured;
				bool fromLeaf;
				traverseAndInsert(child, ((NonLeafNodeInt*) child)->level, false, keyPtr, rid, childRestructured, pageIdFromChild, fromLeaf);

				if(childRestructured) {
					//std::cout << "CHILD WAS RESTRUCTURED : new page = " << pageIdFromChild << std::endl;
					if(fromLeaf) {
						//read the pageIdFromChild and set its level to 1
						Page* pageFromChild;
						NonLeafNodeInt* nodeFromChild;
						bufMgr->readPage(file, pageIdFromChild, pageFromChild);
						nodeFromChild = (NonLeafNodeInt*) pageFromChild;
						nodeFromChild->level = 1;

						//unpin the page
						bufMgr->unPinPage(file, pageIdFromChild, true);
					}
					//if there is room we can just add the key here and shift everything over
					if(nodeInt->keyArray[nodeOccupancy - 1] == INT_MAX) {
						restructured = false;

						insertIntoNonLeafPage(page, (void*) &middleInt, pageIdFromChild);

					} else {
						restructured = true;
						//save the value of middleInt that the previous restructure set
						int middleIntFromChild = middleInt;

						//
						restructure(page, false, (void*) &middleIntFromChild, pageIdFromChild, newPageId);

						//only need to insert if 
						if(middleIntFromChild < middleInt) {
							//insert it onto old node (nodeInt)
							insertIntoNonLeafPage(page, (void*) &middleIntFromChild, pageIdFromChild);
						} else if(middleIntFromChild > middleInt) {
							//insert it onto the new node the child created
							//read in that page
							Page* newNodePage;
							bufMgr->readPage(file, newPageId, newNodePage);

							insertIntoNonLeafPage(newNodePage, (void*) &middleIntFromChild, pageIdFromChild);

							//TODO: unpin that page
						}
					}
				}
			}
			else {
				comingFromLeaf = true;
				//we are the parent of the leaf
				//find what page the leaf is on
				int idx = findIndexIntoPageNoArray(page, keyPtr);
				
				//std::cout << idx << "   ";

				//read in the leaf page and cast
				Page* leafPage;
				PageId leafPageId = nodeInt->pageNoArray[idx];
				bufMgr->readPage(file, leafPageId, leafPage);
				LeafNodeInt* leaf = (LeafNodeInt*) leafPage;

				//find the index into the key array where the rid would go
				int index = findIndexIntoKeyArray(leafPage, keyPtr);

				//std::cout << index << "   ";

				//try to insert into the leaf page
				//if the last place in the leaf is NULL then we dont have to restructure 
				if(leaf->keyArray[leafOccupancy - 1] == INT_MAX) {
					restructured = false;

					//move entries over one place (start at the end)
					for(int i = leafOccupancy - 1; i < index; i--) {
						leaf->keyArray[i] = leaf->keyArray[i - 1];
						leaf->ridArray[i] = leaf->ridArray[i - 1];
					}

					//actually insert the entry
					//std::cout << "INSERTING1: " << key << " on page No. " << nodeInt->pageNoArray[idx] << std::endl;
					leaf->keyArray[index] = key;
					leaf->ridArray[index].page_number = rid.page_number;
					leaf->ridArray[index].slot_number = rid.slot_number;
				} else {
					restructured = true;

					/*
					std::cout << "LEAF PAGE " << leafPageId << " BEFORE RESTRUCTURE" << std::endl;
					Page* prevPage;
					bufMgr->readPage(file, leafPageId, prevPage);
					LeafNodeInt* mynode = (LeafNodeInt*) prevPage;
					for(int j = 0; j < leafOccupancy; j++) {
						if(mynode->keyArray[j] != INT_MAX) std::cout << j << ":" << mynode->keyArray[j] << " ~ ";
					}
					std::cout << " " << std::endl;
					*/

					restructure(leafPage, true, keyPtr, NULL, newPageId);

					/*
					std::cout << "LEAF PAGE " << leafPageId << " AFTER RESTRUCTURE" << std::endl;
					bufMgr->readPage(file, leafPageId, prevPage);
					mynode = (LeafNodeInt*) prevPage;
					for(int j = 0; j < leafOccupancy; j++) {
						if(mynode->keyArray[j] != INT_MAX) std::cout << j << ":" << mynode->keyArray[j] << " ~ ";
					}
					std::cout << " " << std::endl;

					std::cout << "NEW LEAF PAGE " << newPageId << " AFTER RESTRUCTURE" << std::endl;
					bufMgr->readPage(file, newPageId, prevPage);
					mynode = (LeafNodeInt*) prevPage;
					for(int j = 0; j < leafOccupancy; j++) {
						if(mynode->keyArray[j] != INT_MAX) std::cout << j << ":" << mynode->keyArray[j] << " ~ ";
					}
					std::cout << " " << std::endl;

					std::cout << "MIDDLE VALUE = " << middleInt << std::endl;
					*/
					//now actually put the entry passed in on one of these pages
					if(*((int*) keyPtr) >= middleInt) {
						Page* newLeafPage;
						bufMgr->readPage(file, newPageId, newLeafPage);

						LeafNodeInt* newLeaf = (LeafNodeInt*) newLeafPage;

						int index = findIndexIntoKeyArray(newLeafPage, keyPtr);

						//move the keys over on this page
						for(int i = leafOccupancy - 1; i <= index; i--) {
							newLeaf->keyArray[i] = newLeaf->keyArray[i - 1];
							newLeaf->ridArray[i].page_number = newLeaf->ridArray[i - 1].page_number;
							newLeaf->ridArray[i].slot_number = newLeaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						//std::cout << "INSERTING2: " << key << " on page No. " << newPageId << std::endl;
						newLeaf->keyArray[index] = key;
						newLeaf->ridArray[index].page_number = rid.page_number;
						newLeaf->ridArray[index].slot_number = rid.slot_number;
					} else {
						int index = findIndexIntoKeyArray(leafPage, keyPtr);
						
						//move the keys over on this page
						for(int i = leafOccupancy - 1; i <= index; i--) {
							leaf->keyArray[i] = leaf->keyArray[i - 1];
							leaf->ridArray[i].page_number = leaf->ridArray[i - 1].page_number;
							leaf->ridArray[i].slot_number = leaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						//std::cout << "INSERTING3: " << key << " on page No. " << leafPageId << std::endl;
						leaf->keyArray[index] = key;
						leaf->ridArray[index].page_number = rid.page_number;
						leaf->ridArray[index].slot_number = rid.slot_number;
					}
				}
			}

			break;
		}
		case DOUBLE: {
			break;
		}
		case STRING: {
			break;
		}
		default:
			break;

	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::findIndexIntoKeyArray (assumes leaf page)
// -----------------------------------------------------------------------------
int BTreeIndex::findIndexIntoKeyArray(Page* page, const void* keyPtr) {
	switch(attributeType) {
		case INTEGER: {
			int key = *((int*) keyPtr);
			LeafNodeInt* leaf = (LeafNodeInt*) page;
			for(int i = 0; i < leafOccupancy; i++) {
				if(leaf->keyArray[0] == INT_MAX || key < leaf->keyArray[0]) return 0;
				else if(key > leaf->keyArray[i] && i != leafOccupancy - 1 && key < leaf->keyArray[i + 1]) return i + 1;
				else if(key > leaf->keyArray[i] && (i == leafOccupancy - 1 || leaf->keyArray[i + 1] == INT_MAX)) return i + 1;
			}
			break;
		}
		case DOUBLE: {
		
			break;
		}
		case STRING: {
		
			break;
		}
		default: {break;}
	}

	//should always return in the switch statement
	return -1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::findIndexIntoPageNoArray (assumes non leaf page)
// -----------------------------------------------------------------------------
int BTreeIndex::findIndexIntoPageNoArray(Page* page, const void* keyPtr) {
	switch(attributeType) {
		case INTEGER: {
			int key = *((int*) keyPtr);
			NonLeafNodeInt* nodeInt = (NonLeafNodeInt*) page;

			for(int i = 0; i < nodeOccupancy; i++) {
				if(key < nodeInt->keyArray[0]) {
					return 0; 
				}
				else if(key >= nodeInt->keyArray[i] && !(i == nodeOccupancy - 1 || nodeInt->keyArray[i+1] == INT_MAX) && key < nodeInt->keyArray[i + 1]) {
					return i + 1;
				}
				else if(key >= nodeInt->keyArray[i] && (i == nodeOccupancy - 1 || nodeInt->keyArray[i + 1] == INT_MAX)) {
					return i + 1;
				}
			}

			break;
		}
		case DOUBLE: {
		
			break;
		}
		case STRING: {
			
			break;
		}
		default: {break;}
	}

	//should always return in the switch statement
	return -1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::findMiddleValue (called by restructure)
// -----------------------------------------------------------------------------
void BTreeIndex::findMiddleValue(Page* page, bool isLeaf, const void* keyPtr, int &middleIndex) {
	switch(attributeType) {
		case INTEGER: {
			if(isLeaf) {
				LeafNodeInt* leaf = (LeafNodeInt*) page;
				int key = *((int*) keyPtr);
				if(leafOccupancy % 2 == 0) {
					if(key > leaf->keyArray[leafOccupancy/2 - 1] && key < leaf->keyArray[leafOccupancy/2]) {
						middleInt = key;
						middleIndex = leafOccupancy/2;
					}
					else if(key > leaf->keyArray[leafOccupancy/2]) {
						middleInt = (leaf->keyArray[leafOccupancy/2]);
						middleIndex = leafOccupancy/2;
					}
					else {
						middleInt = (leaf->keyArray[leafOccupancy/2 - 1]);
						middleIndex = leafOccupancy/2 - 1;
					}
				} else {
					middleInt = (leaf->keyArray[leafOccupancy/2]);
					middleIndex = leafOccupancy/2;
				}
			} else {
				NonLeafNodeInt* node = (NonLeafNodeInt*) page;
				int key = *((int*) keyPtr);
				if(nodeOccupancy % 2 == 0) {
					if(key > node->keyArray[nodeOccupancy/2 - 1] && key < node->keyArray[nodeOccupancy/2]) {
						middleInt = key;
						middleIndex = nodeOccupancy/2 - 1;
					}
					else if(key > node->keyArray[nodeOccupancy/2]) {
						middleInt = (node->keyArray[nodeOccupancy/2]);
						middleIndex = nodeOccupancy/2;
					}
					else if(key < node->keyArray[nodeOccupancy/2 - 1]){
						middleInt = (node->keyArray[nodeOccupancy/2 - 1]);
						middleIndex = nodeOccupancy/2 - 1;
					} else {
						//ERROR it equals some key passed it. Too late to throw error here as the data is already in the database 
					}
				} else {
					if(key > node->keyArray[nodeOccupancy/2-1] && key < node->keyArray[nodeOccupancy/2]) {
						middleInt = key;
						middleIndex = nodeOccupancy/2 - 1;
					} else if(key > node->keyArray[nodeOccupancy/2] && key < node->keyArray[nodeOccupancy/2 + 1]) {
						middleInt = key;
						middleIndex = nodeOccupancy/2;
					} else if(key < node->keyArray[nodeOccupancy/2-1]) {
						middleInt = (node->keyArray[nodeOccupancy/2-1]);
						middleIndex = nodeOccupancy/2 - 1;
					} else if(key > node->keyArray[nodeOccupancy/2]) {
						middleInt = (node->keyArray[nodeOccupancy/2]);
						middleIndex = nodeOccupancy/2;
					} else {
						//ERROR
					}
				}
			}
			break;
		}
		case DOUBLE: {
			if(isLeaf) {
				if(leafOccupancy % 2 == 0) {

				} else {

				}
			} else {
				if(nodeOccupancy % 2 == 0) {

				} else {

				}
			}
			break;
		}
		case STRING: {
			if(isLeaf) {
				if(leafOccupancy % 2 == 0) {

				} else {

				}
			} else {
				if(nodeOccupancy % 2 == 0) {

				} else {

				}
			}
			break;
		}
		default: {break; }
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::traverse
// -----------------------------------------------------------------------------
const void BTreeIndex::traverse(Page* page, int pageLevel, const void* keyPtr, PageId &leafId) {
	switch(attributeType) {
		case INTEGER: {
			if(pageLevel == 0) {
				//find the index based on the key pointer
				int index = findIndexIntoPageNoArray(page, keyPtr);

				//read in that page and traverse down
				Page* child;
				bufMgr->readPage(file, ((NonLeafNodeInt*) page)->pageNoArray[index], child);
				traverse(child, ((NonLeafNodeInt*) child)->level, keyPtr, leafId);

				//unpin the node page
				bufMgr->unPinPage(file, ((NonLeafNodeInt*) page)->pageNoArray[index], false);
			} else {
				//page is one above the leaf level
				int index = findIndexIntoPageNoArray(page, keyPtr);
				leafId = ((NonLeafNodeInt*) page)->pageNoArray[index];
			}
			break;
		}
		case DOUBLE: {

			break;
		}
		case STRING: {

			break;
		}
		default: { break; }
	}
}
}
