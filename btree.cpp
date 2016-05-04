/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <limits.h>
#include <float.h>
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
#include "exceptions/duplicate_key_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
BTreeIndex::BTreeIndex(const std::string & relationName, std::string & outIndexName, BufMgr *bufMgrIn, const int attrByteOffset, const Datatype attrType) {
    //create the filename
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();
	
	//set values of the private variables
	this->bufMgr = bufMgrIn;
    this->attributeType = attrType;
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

    //Pointers to rootPage and metadata information
	Page* metadataPage;
	IndexMetaInfo* metadata;
	BlobFile* bFile;

	//check if that file exists by creating a new one and having it throw an exception
    try {
		bFile = new BlobFile(outIndexName, true/*try to create a new one*/);
	} catch(const FileExistsException &e) {
		//it already exists so just read it and cast it
		bFile = new BlobFile(outIndexName, false);
		file = (File*) bFile;

		//read the first page which contains metadata information
		bufMgr->readPage(file, headerPageNum, metadataPage);
		metadata = (IndexMetaInfo*) metadataPage;

		//make sure the metadata matches whats passed in if the file already exists
		if(metadata->attrType != attrType ||
			metadata->attrByteOffset != attrByteOffset ||
			strcmp(metadata->relationName, relationName.c_str()) != 0) {

			//if something doesnt match, then throw an exception
			throw BadIndexInfoException("Info passed into constructor doesn't match meta info page");
		}

		//set the root page for this index
		rootPageNum = metadata->rootPageNo;

		//we dont need the header information anymore and we didnt change anything on that page
		bufMgr->unPinPage(file, headerPageNum, false);

		//we are going to keep the rootPage in memory
		bufMgr->readPage(file, rootPageNum, rootPage);

		return;
	}

	//if the code reaches here then the file didnt exist but we created one
	file = (File*) bFile;
	
	//make a metadata Page for this new index, should be the first page
	PageId metadataPageId;
	bufMgr->allocPage(file, metadataPageId, metadataPage);
	headerPageNum = metadataPageId; //just in case it isnt 1 and we need to get at it later save where it is
	metadata = (IndexMetaInfo*) metadataPage;
	
	//set variables in the metadata page
	strncpy(metadata->relationName, relationName.c_str(), 20);
	metadata->attrType = attrType;
	metadata->attrByteOffset = attrByteOffset;

	//create a new root page
	bufMgr->allocPage(file, rootPageNum, rootPage);
	metadata->rootPageNo = rootPageNum;

	std::cout << "ROOT PAGE NUM = " << rootPageNum << std::endl;
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
			PageId leftLeafPageId, rightLeafPageId;

			bufMgr->allocPage(file, leftLeafPageId, leftLeafPage);
			bufMgr->allocPage(file, rightLeafPageId, rightLeafPage);
			
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
			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++) rootNode->keyArray[i] = DBL_MAX;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty left leaf page and right leaf page of the attribute type
			Page* leftLeafPage, *rightLeafPage; 
			PageId leftLeafPageId, rightLeafPageId;

			bufMgr->allocPage(file, leftLeafPageId, leftLeafPage);
			bufMgr->allocPage(file, rightLeafPageId, rightLeafPage);
			
			LeafNodeDouble* leftLeafNode = (LeafNodeDouble*) leftLeafPage;
			LeafNodeDouble* rightLeafNode = (LeafNodeDouble*) rightLeafPage;
			
			//initialize the leaf page
			for(int i = 0; i < leafOccupancy; i++) {
				leftLeafNode->keyArray[i] = DBL_MAX;
				rightLeafNode->keyArray[i] = DBL_MAX;
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
		case Datatype::STRING: {
			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeString* rootNode = (NonLeafNodeString*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++) strncpy(rootNode->keyArray[i], "", STRINGSIZE);
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty left leaf page and right leaf page of the attribute type
			Page* leftLeafPage, *rightLeafPage; 
			PageId leftLeafPageId, rightLeafPageId;

			bufMgr->allocPage(file, leftLeafPageId, leftLeafPage);
			bufMgr->allocPage(file, rightLeafPageId, rightLeafPage);
			
			LeafNodeString* leftLeafNode = (LeafNodeString*) leftLeafPage;
			LeafNodeString* rightLeafNode = (LeafNodeString*) rightLeafPage;
			
			//initialize the leaf page
			for(int i = 0; i < leafOccupancy; i++) {
				strncpy(leftLeafNode->keyArray[i], "", STRINGSIZE);
                strncpy(rightLeafNode->keyArray[i], "", STRINGSIZE);
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

			switch(attrType) {
				case INTEGER:
					insertEntry((void*) (recordPtr + attrByteOffset), rid);
					break;
				case DOUBLE:
					insertEntry((void*) (recordPtr + attrByteOffset), rid);
					break;
				case STRING: {
					char keyBuf[STRINGSIZE + 1];
                    char* src = (char*)(recordPtr + attrByteOffset);
                    for(int i = 0; i < STRINGSIZE; src++, i++) keyBuf[i] = *(src);
                    keyBuf[STRINGSIZE] = '\0';
                    std::string key = std::string(keyBuf);
					insertEntry((void*) &key, rid);
					break;
                }
				default: { break; }
			}
		}
	} catch (EndOfFileException &e) {
		//end of the scan has been reached
	}

	delete fileScan;
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
			std::cout << "ScanNotInitializedException thrown in BTreeIndex destructor\n"; 
		}
	}
	
	bufMgr->unPinPage(file, rootPageNum, true);

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
	//root page should already be in the buffer

	//cast the rootPage to a non leaf node depending on type
	switch(attributeType) {
		case Datatype::INTEGER: {
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
			bool restructured;
			bool comingFromLeaf;
			PageId newPageId;

			traverseAndInsert(rootPage, rootNode->level, true, key, rid, restructured, newPageId, comingFromLeaf);

			//if the root was restructured then update the metapage!!!
			if(restructured) {
				if(rootNode->keyArray[nodeOccupancy - 1] == INT_MAX) {
                    //we have room so just put it on this page
					insertIntoNonLeafPage(rootPage, (void*) &middleInt, newPageId);

				} else {
					PageId addedPageId;
					restructure(rootPage, false, (void*) &middleInt, newPageId, addedPageId);

					//create a new NonLeafPage and put the middle int on it
					Page* newRootPage;
					PageId newRootPageId;
					bufMgr->allocPage(file, newRootPageId, newRootPage);
					NonLeafNodeInt* newRoot = (NonLeafNodeInt*) newRootPage;

					//we know this can never be just above the leaves so set level to 0
					newRoot->level = 0;

					//null eveything in this new page
					newRoot->pageNoArray[nodeOccupancy] = NULL;
					for(int i = 0; i < nodeOccupancy; i++) {
						newRoot->keyArray[i] = INT_MAX;
						newRoot->pageNoArray[i] = NULL;
					}

					//the only value in the new root is the middle value passed up from the child
					newRoot->keyArray[0] = middleInt;

					//the left child is the old root page
					newRoot->pageNoArray[0] = rootPageNum;

					//the right child is the one that was added by the restructure method
					newRoot->pageNoArray[1] = addedPageId;

					//unpin the old root page and update the class references
					bufMgr->unPinPage(file, rootPageNum, true);
					rootPageNum = newRootPageId;
					rootPage = newRootPage;

                    //update the meta info
                    //read in the metainfo so it can be updated
                    Page* metadataPage;
					
                    bufMgr->readPage(file, headerPageNum, metadataPage);
                    IndexMetaInfo* metadata = (IndexMetaInfo*) metadataPage;

                    metadata->rootPageNo = newRootPageId;

                    //unpin the metadataPage
                    bufMgr->unPinPage(file, headerPageNum, true);
				}
			}

			break;
		}
		case Datatype::DOUBLE: {
			NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*) rootPage;
			bool restructured;
			bool comingFromLeaf;
			PageId newPageId;

			traverseAndInsert(rootPage, rootNode->level, true, key, rid, restructured, newPageId, comingFromLeaf);
             
			//TODO: if the root was restructured then update the metapage!!!
			if(restructured) {
				if(rootNode->keyArray[nodeOccupancy - 1] == DBL_MAX) {
                    //we have room so just put it on this page
					insertIntoNonLeafPage(rootPage, (void*) &middleDouble, newPageId); 
				} else {
                    //TODO: FIXME copy from int
				}
			}

            break;
		}
		case Datatype::STRING: {
            NonLeafNodeString* rootNode = (NonLeafNodeString*) rootPage;
			bool restructured;
			bool comingFromLeaf;
			PageId newPageId;

			traverseAndInsert(rootPage, rootNode->level, true, key, rid, restructured, newPageId, comingFromLeaf);

			//if the root was restructured then update the metapage!!!
			if(restructured) {
				if(strcmp(rootNode->keyArray[nodeOccupancy - 1], "") == 0) {
                    //we have room so just put it on this page
					insertIntoNonLeafPage(rootPage, (void*) &middleString, newPageId);

				} else {

                    // TODO: change to strings!!!!!
				}
			}
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

	//root page should already be pinned in the bufMgr
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
            currentPageNum = leafPageId;

			//find the first record then set the class variables
			bool firstRecordFound = false;
			while(!firstRecordFound) {
				//loop through all the keys on the leaf
				for(int i = 0; i < leafOccupancy; i++) {

                    //for any non-NULL key, check if the value is in the range
					if(leaf->keyArray[i] != INT_MAX && 
						((lowOp == GT && leaf->keyArray[i] > lowValInt) || (lowOp == GTE && leaf->keyArray[i] >= lowValInt)) && 
						((highOp == LT && leaf->keyArray[i] < highValInt) || (highOp == LTE && leaf->keyArray[i] <= highValInt))) {
						
                        //we have found the first value in the range so set the state variables
                        currentPageData = leafPage;
						currentPageNum = leafPageId;
						nextEntry = i;
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

						//unpin the current page
						bufMgr->unPinPage(file, leafPageId, false);

						nextEntry = -1;
						firstRecordFound = true; //just to break out of the loop
                        throw NoSuchKeyFoundException();
					}
					
				}
			}

			break;
		}
		case DOUBLE: {
		    lowValDouble = *((double*) lowValParm);
			highValDouble = *((double*) highValParm);

			// Method throws exception if lower bound > upper bound
			if(lowValDouble > highValDouble) {
				throw BadScanrangeException();
			}

			//traverse to get to the leafPageId
			Page* leafPage;
			PageId leafPageId;
			traverse(rootPage, ((NonLeafNodeDouble*) rootPage)->level, lowValParm, leafPageId);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeDouble* leaf = (LeafNodeDouble*) leafPage;
            currentPageNum = leafPageId;

			//find the first record then set the class variables
			bool firstRecordFound = false;
			while(!firstRecordFound) {
				//loop through all the keys on the leaf
				for(int i = 0; i < leafOccupancy; i++) {

                    //for any non-NULL key, check if the value is in the range
					if(leaf->keyArray[i] != DBL_MAX && 
						((lowOp == GT && leaf->keyArray[i] > lowValDouble) || (lowOp == GTE && leaf->keyArray[i] >= lowValDouble)) && 
						((highOp == LT && leaf->keyArray[i] < highValDouble) || (highOp == LTE && leaf->keyArray[i] <= highValDouble))) {
						
                        //we have found the first key in the range so set the state variables
						currentPageData = leafPage;
						currentPageNum = leafPageId;
						nextEntry = i;
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
						leaf = (LeafNodeDouble*) leafPage;
					} else {
						//we've reached the end of our data and still havent found anything greater than the lowParm

						//unpin the current page
						bufMgr->unPinPage(file, leafPageId, false);

						nextEntry = -1;
						firstRecordFound = true; //just to break out of the loop
                        throw NoSuchKeyFoundException();
					}
					
				}
			}
			break;
		}
		case STRING: {
			char keyBufLow[STRINGSIZE + 1];
            char* src = (char*)(lowValParm);
            for(int i = 0; i < STRINGSIZE; src++, i++) keyBufLow[i] = *(src);
            keyBufLow[STRINGSIZE] = '\0';
            lowValString.assign(std::string(keyBufLow));

			char keyBufHigh[STRINGSIZE + 1];
            src = (char*)(highValParm);
            for(int i = 0; i < STRINGSIZE; src++, i++) keyBufHigh[i] = *(src);
            keyBufHigh[STRINGSIZE] = '\0';
            highValString.assign(std::string(keyBufHigh)); 

			// Method throws exception if lower bound > upper bound
			if(strcmp(lowValString.c_str(), highValString.c_str()) > 0) {
				throw BadScanrangeException();
			}

			//traverse to get to the leafPageId
			Page* leafPage;
			PageId leafPageId;
			traverse(rootPage, ((NonLeafNodeString*) rootPage)->level, (void*) &lowValString, leafPageId);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeString* leaf = (LeafNodeString*) leafPage;
            currentPageNum = leafPageId;
            

			//find the first record then set the class variables
			bool firstRecordFound = false;
			while(!firstRecordFound) {
				//loop through all the keys on the leaf
				for(int i = 0; i < leafOccupancy; i++) {
                    
                    //copy the first 10 characters into a null-terminated string buffer
                    char leafKey[STRINGSIZE + 1];
                    src = (char*)(leaf->keyArray[i]);
                    for(int i = 0; i < STRINGSIZE; src++, i++) leafKey[i] = *(src);
                    leafKey[STRINGSIZE] = '\0';

					if(!std::string(leaf->keyArray[i]).empty() && 
						((lowOp == GT  && strcmp(std::string(leafKey).c_str(), lowValString.c_str())  > 0) || (lowOp == GTE  && strcmp(std::string(leafKey).c_str(), lowValString.c_str())  >= 0)) && 
						((highOp == LT && strcmp(std::string(leafKey).c_str(), highValString.c_str()) < 0) || (highOp == LTE && strcmp(std::string(leafKey).c_str(), highValString.c_str()) <= 0))) {
						
                        //we have found the first record in the range so set the state variables
                        currentPageData = leafPage;
						currentPageNum = leafPageId;
						nextEntry = i;
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
						leaf = (LeafNodeString*) leafPage;
					} else {
						//we've reached the end of our data and still havent found anything greater than the lowParm

						//unpin the current page
						bufMgr->unPinPage(file, leafPageId, false);

						nextEntry = -1;
						firstRecordFound = true; //just to break out of the loop
                        throw NoSuchKeyFoundException();
					}
				}
			}

			break;
		}
		default: { break; }
	
	}


	//after we know everything is good to go, then set scanExecuting = true
	scanExecuting = true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(!scanExecuting) throw ScanNotInitializedException();

    //if next entry was set to -1 in the previous scan next (or start scan) then we are done scanning so throw the exception
	if(nextEntry == -1) throw  IndexScanCompletedException();

	//current page should already be read in and referenced
	switch(attributeType) {
		case INTEGER: {
			LeafNodeInt* leaf = (LeafNodeInt*) currentPageData;
			outRid = leaf->ridArray[nextEntry];

			if(nextEntry == leafOccupancy - 1 || leaf->keyArray[nextEntry+1] == INT_MAX) {
				//bring in the next page if we can 
				if(leaf->rightSibPageNo != NULL) {
					Page* nextPage;
					PageId newPageId = leaf->rightSibPageNo;
					bufMgr->readPage(file, newPageId, nextPage);

					//unpin the previous page
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageData = nextPage;
					currentPageNum = newPageId;

					leaf = (LeafNodeInt*) nextPage;
                
                    //check if the next value on the new page is still within the criteria for the scan
					if((highOp == LT && leaf->keyArray[0] < highValInt) || (highOp == LTE && leaf->keyArray[0] <= highValInt)) {
						nextEntry = 0;
					} else {
						nextEntry = -1;
					}
				} else {
                    //if there is no next page then set nextEntry to -1
					nextEntry = -1;
				}
				
			} else {
                //normal operation, just see if the next entry matches the scan criteria
				if((highOp == LT && leaf->keyArray[nextEntry+1] < highValInt) || (highOp == LTE && leaf->keyArray[nextEntry+1] <= highValInt)) {
					nextEntry++;
				} else {
					nextEntry = -1;
				}
			}

			break;
		}
		case DOUBLE: {
			LeafNodeDouble* leaf = (LeafNodeDouble*) currentPageData;
			outRid = leaf->ridArray[nextEntry];

			if(nextEntry == leafOccupancy - 1 || leaf->keyArray[nextEntry+1] == DBL_MAX) {
				//bring in the next page if we can
				if(leaf->rightSibPageNo != NULL) {
					Page* nextPage;
					PageId newPageId = leaf->rightSibPageNo;
					bufMgr->readPage(file, newPageId, nextPage);

					//unpin the previous page
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageData = nextPage;
					currentPageNum = newPageId;

					leaf = (LeafNodeDouble*) nextPage;

                    //check if the next value on the new page is still within the criteria for the scan
					if((highOp == LT && leaf->keyArray[0] < highValDouble) || (highOp == LTE && leaf->keyArray[0] <= highValDouble)) {
						nextEntry = 0;
					} else {
						nextEntry = -1;
					}
				} else {
                    //if there is no next page then set nextEntry to -1 
					nextEntry = -1;
				}
				
			} else {
                //normal operation, just see if the next entry matches the scan criteria
				if((highOp == LT && leaf->keyArray[nextEntry+1] < highValDouble) || (highOp == LTE && leaf->keyArray[nextEntry+1] <= highValDouble)) {
					nextEntry++;
				} else {
					nextEntry = -1;
				}
			}
		
			break;
		} 
		case STRING: { 
		    LeafNodeString* leaf = (LeafNodeString*) currentPageData;
			outRid = leaf->ridArray[nextEntry];

			if(nextEntry == leafOccupancy - 1 || strcmp(leaf->keyArray[nextEntry+1], "") == 0) {
				//bring in the next page if we can
				if(leaf->rightSibPageNo != NULL) {
					Page* nextPage;
					PageId newPageId = leaf->rightSibPageNo;
					bufMgr->readPage(file, newPageId, nextPage);

					//unpin the previous page
					bufMgr->unPinPage(file, currentPageNum, false);
					currentPageData = nextPage;
					currentPageNum = newPageId;

					leaf = (LeafNodeString*) nextPage;

                    char leafKey[STRINGSIZE + 1];
                    char* src = (char*)(leaf->keyArray[0]);
                    for(int i = 0; i < STRINGSIZE; src++, i++) leafKey[i] = *(src);
                    leafKey[STRINGSIZE] = '\0';

                    //check if the next value on the new page is still within the criteria for the scan
					if((highOp == LT && strcmp(std::string(leafKey).c_str(), highValString.c_str()) < 0) || (highOp == LTE && strcmp(std::string(leafKey).c_str(), highValString.c_str()) <= 0)) {
						nextEntry = 0;
					} else {
						nextEntry = -1;
					}
				} else {
                    //if there is no next page then set nextEntry to -1 
					nextEntry = -1;
				}
				
			} else {
                char leafKey[STRINGSIZE + 1];
                char* src = (char*)(leaf->keyArray[nextEntry + 1]);
                for(int i = 0; i < STRINGSIZE; src++, i++) leafKey[i] = *(src);
                leafKey[STRINGSIZE] = '\0';

                //normal operation, just see if the next entry matches the scan criteria
				if((highOp == LT && strcmp(std::string(leafKey).c_str(), highValString.c_str()) < 0) || (highOp == LTE && strcmp(std::string(leafKey).c_str(), highValString.c_str()) <= 0)) {
					nextEntry++;
				} else {
					nextEntry = -1;
				}
			}
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
	//try {
		bufMgr->unPinPage(file, currentPageNum, false);
	//} catch(const PageNotPinnedException &e) {
	//	std::cout << "PageNotPinned thrown in endScan()\n";
	//}
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

            //find where the key would go and move all the entries over from that point until the end
			for(int i = 0; i < nodeOccupancy; i++) {
				if(node->keyArray[i] == INT_MAX) {
					node->keyArray[i] = key;
					node->pageNoArray[i+1] = pageId;
					break;
				} else if(key < node->keyArray[i]) {
					//move everything over to the right
					for(int j = nodeOccupancy - 1; j > i; j--) {
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
			NonLeafNodeDouble* node = (NonLeafNodeDouble*) page;
			double key = *((double*) keyPtr);

            //find where the key would go and move all the entries over from that point until the end
			for(int i = 0; i < nodeOccupancy; i++) {
				if(node->keyArray[i] == DBL_MAX) {
					node->keyArray[i] = key;
					node->pageNoArray[i+1] = pageId;
					break;
				} else if(key < node->keyArray[i]) {
					//move everything over to the right
					for(int j = nodeOccupancy - 1; j > i; j--) { // CHANGED TO j>i 
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
		case STRING: {
			NonLeafNodeString* node = (NonLeafNodeString*) page;
			std::string key = *((std::string*) keyPtr);
        
            //find where the key would go and move all the entries over from that point until the end
			for(int i = 0; i < nodeOccupancy; i++) {
				if(strcmp(node->keyArray[i], "") == 0) {
					strncpy(node->keyArray[i], key.c_str(), STRINGSIZE);
					node->pageNoArray[i+1] = pageId;
					break;
				} else if(strcmp(key.c_str(), node->keyArray[i]) < 0) {
					//move everything over to the right
					for(int j = nodeOccupancy - 1; j > i; j--) {
						strncpy(node->keyArray[j], node->keyArray[j-1], STRINGSIZE);
						node->pageNoArray[j+1] = node->pageNoArray[j];
					}
					strncpy(node->keyArray[i], key.c_str(), STRINGSIZE);
					node->pageNoArray[i+1] = pageId;
					break;
				}
			}
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

				//unpin the page that was created
				bufMgr->unPinPage(file, newPageId, true);
			} else {
				//cast the fullPage to a nonleaf
				NonLeafNodeInt* fullNode = (NonLeafNodeInt*) fullPage;

				//create a new page
				Page* newNodePage;
				bufMgr->allocPage(file, newPageId, newNodePage);
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
					newNode->pageNoArray[nodeOccupancy-middleIndex-1] = fullNode->pageNoArray[nodeOccupancy];
				}

				//unpin the page that was created
				bufMgr->unPinPage(file, newPageId, true);
			}
			break;
		}
		case DOUBLE: {
		    if(isLeaf) {
				//cast the fullPage to a leaf
				LeafNodeDouble* fullLeaf = (LeafNodeDouble*) fullPage;

				//create a new page
				Page* newLeafPage;
				bufMgr->allocPage(file, newPageId, newLeafPage);
				LeafNodeDouble* newLeaf = (LeafNodeDouble*) newLeafPage;

				//NULL everything in the new page
				for(int i = 0; i < leafOccupancy; i++) {
					newLeaf->keyArray[i] = DBL_MAX;
					//not NULLing the rids here because we just assume if the key is NULL then so is the associated rid so don't access it
				}

				//get the middle value and index from the page
				int middleIndex;
				findMiddleValue(fullPage, true, keyPtr, middleIndex);

				//copy all the keys and rids over from middleIndex
				for(int i = middleIndex; i < leafOccupancy; i++) {
					newLeaf->keyArray[i-middleIndex] = fullLeaf->keyArray[i];
					fullLeaf->keyArray[i] = DBL_MAX;
					newLeaf->ridArray[i-middleIndex] = fullLeaf->ridArray[i];
					//not NULLing rids here again, just check if corresponding key is null to see if the data is valid
				}

				newLeaf->rightSibPageNo = fullLeaf->rightSibPageNo;
				fullLeaf->rightSibPageNo = newPageId;

				//unpin the page that was created
				bufMgr->unPinPage(file, newPageId, true);
			} else {
				//cast the fullPage to a nonleaf
				NonLeafNodeDouble* fullNode = (NonLeafNodeDouble*) fullPage;

				//create a new page
				Page* newNodePage;
				bufMgr->allocPage(file, newPageId, newNodePage);
				NonLeafNodeDouble* newNode = (NonLeafNodeDouble*) newNodePage;

				//NULL everything in the new page 
				newNode->pageNoArray[nodeOccupancy] = NULL;
				for(int i = 0; i < nodeOccupancy; i++) {
					newNode->keyArray[i] = DBL_MAX;
					newNode->pageNoArray[i] = NULL;
				}

				//get the middle value and index from the page
				int middleIndex;
				findMiddleValue(fullPage, false, keyPtr, middleIndex);

				//if the keyPtr we are trying to insert is the middle one the some special stuff happens
				if(*((double*) keyPtr) == middleDouble) {
					newNode->pageNoArray[0] = newPageIdFromChild;

					for(int i = middleIndex + 1; i < nodeOccupancy; i++) {
						newNode->keyArray[i-middleIndex-1] = fullNode->keyArray[i];
						fullNode->keyArray[i] = DBL_MAX;
						newNode->pageNoArray[i-middleIndex] = fullNode->pageNoArray[i+1];
					}
				} else {
					for(int i = middleIndex + 1; i < nodeOccupancy; i++) {
						newNode->keyArray[i-middleIndex-1] = fullNode->keyArray[i];
						fullNode->keyArray[i] = DBL_MAX;
						newNode->pageNoArray[i-middleIndex-1] = fullNode->pageNoArray[i];
					}
					newNode->pageNoArray[nodeOccupancy-middleIndex+1] = fullNode->pageNoArray[nodeOccupancy];
				}

				//unpin the page that was created
				bufMgr->unPinPage(file, newPageId, true);
			}
		
			break;
		}
		case STRING: {
			if(isLeaf) {
				//cast the fullPage to a leaf
				LeafNodeString* fullLeaf = (LeafNodeString*) fullPage;

				//create a new page
				Page* newLeafPage;
				bufMgr->allocPage(file, newPageId, newLeafPage);
				LeafNodeString* newLeaf = (LeafNodeString*) newLeafPage;

				//NULL everything in the new page
				for(int i = 0; i < leafOccupancy; i++) {
					strncpy(newLeaf->keyArray[i], "", STRINGSIZE);
					//not NULLing the rids here because we just assume if the key is NULL then so is the associated rid so don't access it
				}

				//get the middle value and index from the page
				int middleIndex;
				findMiddleValue(fullPage, true, keyPtr, middleIndex);

				//copy all the keys and rids over from middleIndex
				for(int i = middleIndex; i < leafOccupancy; i++) {
					strncpy(newLeaf->keyArray[i-middleIndex], fullLeaf->keyArray[i], STRINGSIZE);
					strncpy(fullLeaf->keyArray[i], "", STRINGSIZE);
					newLeaf->ridArray[i-middleIndex] = fullLeaf->ridArray[i];
					//not NULLing rids here again, just check if corresponding key is null to see if the data is valid
				}

				newLeaf->rightSibPageNo = fullLeaf->rightSibPageNo;
				fullLeaf->rightSibPageNo = newPageId;

				//unpin the page that was created
				bufMgr->unPinPage(file, newPageId, true);
			} else {
				//cast the fullPage to a nonleaf
				NonLeafNodeString* fullNode = (NonLeafNodeString*) fullPage;

				//create a new page
				Page* newNodePage;
				bufMgr->allocPage(file, newPageId, newNodePage);
				NonLeafNodeString* newNode = (NonLeafNodeString*) newNodePage;

				//NULL everything in the new page 
				newNode->pageNoArray[nodeOccupancy] = NULL;
				for(int i = 0; i < nodeOccupancy; i++) {
					strncpy(newNode->keyArray[i], "", STRINGSIZE);
					newNode->pageNoArray[i] = NULL;
				}

				//get the middle value and index from the page
				int middleIndex;
				findMiddleValue(fullPage, false, keyPtr, middleIndex);

				//if the keyPtr we are trying to insert is the middle one the some special stuff happens
				if(strcmp((*((std::string*) keyPtr)).c_str(), middleString.c_str()) == 0) {
					newNode->pageNoArray[0] = newPageIdFromChild;

					for(int i = middleIndex + 1; i < nodeOccupancy; i++) {
						strncpy(newNode->keyArray[i-middleIndex-1], fullNode->keyArray[i], STRINGSIZE);
						strncpy(fullNode->keyArray[i], "", STRINGSIZE);
						newNode->pageNoArray[i-middleIndex] = fullNode->pageNoArray[i+1];
					}
				} else {
					for(int i = middleIndex + 1; i < nodeOccupancy; i++) {
						strncpy(newNode->keyArray[i-middleIndex-1], fullNode->keyArray[i], STRINGSIZE);
						strncpy(fullNode->keyArray[i], "", STRINGSIZE);
						newNode->pageNoArray[i-middleIndex-1] = fullNode->pageNoArray[i];
					}
					newNode->pageNoArray[nodeOccupancy-middleIndex-1] = fullNode->pageNoArray[nodeOccupancy];
				}

				//unpin the page that was created
				bufMgr->unPinPage(file, newPageId, true);
			}
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

						restructure(page, false, (void*) &middleIntFromChild, pageIdFromChild, newPageId);

						//only need to insert if not equal 
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
                            bufMgr->unPinPage(file, newPageId, true);
						}
					}
				}
			}
			else {
				comingFromLeaf = true;
				//we are the parent of the leaf
				//find what page the leaf is on
				int idx = findIndexIntoPageNoArray(page, keyPtr);

				//read in the leaf page and cast
				Page* leafPage;
				PageId leafPageId = nodeInt->pageNoArray[idx];
				bufMgr->readPage(file, leafPageId, leafPage);
				LeafNodeInt* leaf = (LeafNodeInt*) leafPage;

				//find the index into the key array where the rid would go
				int index = findIndexIntoKeyArray(leafPage, keyPtr);

				//try to insert into the leaf page
				//if the last place in the leaf is NULL then we dont have to restructure 
				if(leaf->keyArray[leafOccupancy - 1] == INT_MAX) {
					restructured = false;

					//move entries over one place (start at the end)
					for(int i = leafOccupancy - 1; i > index; i--) {
						leaf->keyArray[i] = leaf->keyArray[i - 1];
						leaf->ridArray[i] = leaf->ridArray[i - 1];
					}

					//actually insert the entry
					leaf->keyArray[index] = key;
					leaf->ridArray[index].page_number = rid.page_number;
					leaf->ridArray[index].slot_number = rid.slot_number;
				} else {
					restructured = true;

					restructure(leafPage, true, keyPtr, NULL, newPageId);
					
					//now actually put the entry passed in on one of these pages
					if(*((int*) keyPtr) >= middleInt) {
						Page* newLeafPage;
						bufMgr->readPage(file, newPageId, newLeafPage);

						LeafNodeInt* newLeaf = (LeafNodeInt*) newLeafPage;

						int index = findIndexIntoKeyArray(newLeafPage, keyPtr);

						//move the keys over on this page
						for(int i = leafOccupancy - 1; i > index; i--) {
							newLeaf->keyArray[i] = newLeaf->keyArray[i - 1];
							newLeaf->ridArray[i].page_number = newLeaf->ridArray[i - 1].page_number;
							newLeaf->ridArray[i].slot_number = newLeaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						newLeaf->keyArray[index] = key;
						newLeaf->ridArray[index].page_number = rid.page_number;
						newLeaf->ridArray[index].slot_number = rid.slot_number;					
						
						//unpin the new leafPage created
						bufMgr->unPinPage(file, newPageId, true);
					} else {
						int index = findIndexIntoKeyArray(leafPage, keyPtr);
						
						//move the keys over on this page
						for(int i = leafOccupancy - 1; i > index; i--) {
							leaf->keyArray[i] = leaf->keyArray[i - 1];
							leaf->ridArray[i].page_number = leaf->ridArray[i - 1].page_number;
							leaf->ridArray[i].slot_number = leaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						leaf->keyArray[index] = key;
						leaf->ridArray[index].page_number = rid.page_number;
						leaf->ridArray[index].slot_number = rid.slot_number;
					}

				}
				//unpin the leaf page
				bufMgr->unPinPage(file, leafPageId, true);
			}

			break;
		}
		case DOUBLE: {
			double key = *((double*) keyPtr);
			NonLeafNodeDouble* nodeDouble = (NonLeafNodeDouble*) page;

			if(isRoot && nodeDouble->keyArray[0] == DBL_MAX) {
				//set the first key in the root 
				nodeDouble->keyArray[0] = key;
			}

			if(pageLevel == 0) {
				comingFromLeaf = false;

				//find where the key would go
				int index = findIndexIntoPageNoArray(page, keyPtr);

				//read in that page
				Page* child;
				bufMgr->readPage(file, nodeDouble->pageNoArray[index], child);

				//recurse into that child
				PageId pageIdFromChild;
				bool childRestructured;
				bool fromLeaf;
				traverseAndInsert(child, ((NonLeafNodeDouble*) child)->level, false, keyPtr, rid, childRestructured, pageIdFromChild, fromLeaf);

				if(childRestructured) {
					if(fromLeaf) {
						//read the pageIdFromChild and set its level to 1
						Page* pageFromChild;
						NonLeafNodeDouble* nodeFromChild;
						bufMgr->readPage(file, pageIdFromChild, pageFromChild);
						nodeFromChild = (NonLeafNodeDouble*) pageFromChild;
						nodeFromChild->level = 1;

						//unpin the page
						bufMgr->unPinPage(file, pageIdFromChild, true);
					}
					//if there is room we can just add the key here and shift everything over
					if(nodeDouble->keyArray[nodeOccupancy - 1] == DBL_MAX) {
						restructured = false;

						insertIntoNonLeafPage(page, (void*) &middleDouble, pageIdFromChild);

					} else {
						restructured = true;
						//save the value of middleDouble that the previous restructure set
						double middleDoubleFromChild = middleDouble;

						restructure(page, false, (void*) &middleDoubleFromChild, pageIdFromChild, newPageId);

						//only need to insert if not equal
						if(middleDoubleFromChild < middleDouble) {
							//insert it onto old node (nodeDouble)
							insertIntoNonLeafPage(page, (void*) &middleDoubleFromChild, pageIdFromChild);
						} else if(middleDoubleFromChild > middleDouble) {
							//insert it onto the new node the child created
							//read in that page
							Page* newNodePage;
							bufMgr->readPage(file, newPageId, newNodePage);

							insertIntoNonLeafPage(newNodePage, (void*) &middleDoubleFromChild, pageIdFromChild);

							//unpin that page
                            bufMgr->unPinPage(file, newPageId, true);
						}
					}
				}
			}
			else {
				comingFromLeaf = true;
				//we are the parent of the leaf
				//find what page the leaf is on
				int idx = findIndexIntoPageNoArray(page, keyPtr);

				//read in the leaf page and cast
				Page* leafPage;
				PageId leafPageId = nodeDouble->pageNoArray[idx];
				bufMgr->readPage(file, leafPageId, leafPage);
				LeafNodeDouble* leaf = (LeafNodeDouble*) leafPage;

				//find the index into the key array where the rid would go
				int index = findIndexIntoKeyArray(leafPage, keyPtr);

				//try to insert into the leaf page
				//if the last place in the leaf is NULL then we dont have to restructure 
				if(leaf->keyArray[leafOccupancy - 1] == DBL_MAX) {
					restructured = false;

					//move entries over one place (start at the end)
					for(int i = leafOccupancy - 1; i > index; i--) {
						leaf->keyArray[i] = leaf->keyArray[i - 1];
						leaf->ridArray[i] = leaf->ridArray[i - 1];
					}

					//actually insert the entry
					leaf->keyArray[index] = key;
					leaf->ridArray[index].page_number = rid.page_number;
					leaf->ridArray[index].slot_number = rid.slot_number;
				} else {
					restructured = true;

					restructure(leafPage, true, keyPtr, NULL, newPageId);
					
					//now actually put the entry passed in on one of these pages
					if(*((double*) keyPtr) >= middleDouble) {
						Page* newLeafPage;
						bufMgr->readPage(file, newPageId, newLeafPage);

						LeafNodeDouble* newLeaf = (LeafNodeDouble*) newLeafPage;

						int index = findIndexIntoKeyArray(newLeafPage, keyPtr);

						//move the keys over on this page
						for(int i = leafOccupancy - 1; i > index; i--) {
							newLeaf->keyArray[i] = newLeaf->keyArray[i - 1];
							newLeaf->ridArray[i].page_number = newLeaf->ridArray[i - 1].page_number;
							newLeaf->ridArray[i].slot_number = newLeaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						newLeaf->keyArray[index] = key;
						newLeaf->ridArray[index].page_number = rid.page_number;
						newLeaf->ridArray[index].slot_number = rid.slot_number;					
						
						//unpin the new leafPage created
						bufMgr->unPinPage(file, newPageId, true);
					} else {
						int index = findIndexIntoKeyArray(leafPage, keyPtr);
						
						//move the keys over on this page
						for(int i = leafOccupancy - 1; i > index; i--) {
							leaf->keyArray[i] = leaf->keyArray[i - 1];
							leaf->ridArray[i].page_number = leaf->ridArray[i - 1].page_number;
							leaf->ridArray[i].slot_number = leaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						leaf->keyArray[index] = key;
						leaf->ridArray[index].page_number = rid.page_number;
						leaf->ridArray[index].slot_number = rid.slot_number;
					}

				}
				//unpin the leaf page
				bufMgr->unPinPage(file, leafPageId, true);
			}

			break;
		}
		case STRING: {
            std::string key = *((std::string*) keyPtr);
			NonLeafNodeString* nodeString = (NonLeafNodeString*) page;

			if(isRoot && strcmp(nodeString->keyArray[0], "") == 0) {
				//set the first key in the root 
				strncpy(nodeString->keyArray[0], key.c_str(), STRINGSIZE);
			}

			if(pageLevel == 0) {
				comingFromLeaf = false;

				//find where the key would go
				int index = findIndexIntoPageNoArray(page, keyPtr);

				//read in that page
				Page* child;
				bufMgr->readPage(file, nodeString->pageNoArray[index], child);

				//recurse into that child
				PageId pageIdFromChild;
				bool childRestructured;
				bool fromLeaf;
				traverseAndInsert(child, ((NonLeafNodeString*) child)->level, false, keyPtr, rid, childRestructured, pageIdFromChild, fromLeaf);

				if(childRestructured) {
					if(fromLeaf) {
						//read the pageIdFromChild and set its level to 1
						Page* pageFromChild;
						bufMgr->readPage(file, pageIdFromChild, pageFromChild);
						NonLeafNodeString* nodeFromChild = (NonLeafNodeString*) pageFromChild;
						nodeFromChild->level = 1;

						//unpin the page
						bufMgr->unPinPage(file, pageIdFromChild, true);
					}
					//if there is room we can just add the key here and shift everything over
					if(strcmp(nodeString->keyArray[nodeOccupancy - 1], "") == 0) {
						restructured = false;

						insertIntoNonLeafPage(page, (void*) &middleString, pageIdFromChild);

					} else {
						restructured = true;
						//save the value of middleInt that the previous restructure set
						std::string middleStringFromChild = middleString;

						restructure(page, false, (void*) &middleStringFromChild, pageIdFromChild, newPageId);

						//only need to insert if 
						if(strcmp(middleStringFromChild.c_str(), middleString.c_str()) < 0) {
							//insert it onto old node 
							insertIntoNonLeafPage(page, (void*) &middleStringFromChild, pageIdFromChild);
						} else if(strcmp(middleStringFromChild.c_str(), middleString.c_str()) > 0) {
							//insert it onto the new node the child created
							//read in that page
							Page* newNodePage;
							bufMgr->readPage(file, newPageId, newNodePage);

							insertIntoNonLeafPage(newNodePage, (void*) &middleStringFromChild, pageIdFromChild);

							//unpin that page
                            bufMgr->unPinPage(file, newPageId, true);
						}
					}
				}
			}
			else {
				comingFromLeaf = true;
				//we are the parent of the leaf
				//find what page the leaf is on
				int idx = findIndexIntoPageNoArray(page, keyPtr);

				//read in the leaf page and cast
				Page* leafPage;
				PageId leafPageId = nodeString->pageNoArray[idx];
				bufMgr->readPage(file, leafPageId, leafPage);
				LeafNodeString* leaf = (LeafNodeString*) leafPage;

				//find the index into the key array where the rid would go
				int index = findIndexIntoKeyArray(leafPage, keyPtr);

				//try to insert into the leaf page
				//if the last place in the leaf is NULL then we dont have to restructure 
				if(strcmp(leaf->keyArray[leafOccupancy - 1], "") == 0) {
					restructured = false;

					//move entries over one place (start at the end)
					for(int i = leafOccupancy - 1; i > index; i--) {
						strncpy(leaf->keyArray[i], leaf->keyArray[i - 1], STRINGSIZE);
						leaf->ridArray[i] = leaf->ridArray[i - 1];
					}

					//actually insert the entry
					strncpy(leaf->keyArray[index], key.c_str(), STRINGSIZE);
					leaf->ridArray[index].page_number = rid.page_number;
					leaf->ridArray[index].slot_number = rid.slot_number;
				} else {
					restructured = true;

					restructure(leafPage, true, keyPtr, NULL, newPageId);
					
					//now actually put the entry passed in on one of these pages
					if(strcmp((*((std::string*) keyPtr)).c_str(), middleString.c_str()) >= 0) {
						Page* newLeafPage;
						bufMgr->readPage(file, newPageId, newLeafPage);

						LeafNodeString* newLeaf = (LeafNodeString*) newLeafPage;

						int index = findIndexIntoKeyArray(newLeafPage, keyPtr);

						//move the keys over on this page
						for(int i = leafOccupancy - 1; i > index; i--) {
							strncpy(newLeaf->keyArray[i], newLeaf->keyArray[i - 1], STRINGSIZE);
							newLeaf->ridArray[i].page_number = newLeaf->ridArray[i - 1].page_number;
							newLeaf->ridArray[i].slot_number = newLeaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						strncpy(newLeaf->keyArray[index], key.c_str(), STRINGSIZE);
						newLeaf->ridArray[index].page_number = rid.page_number;
						newLeaf->ridArray[index].slot_number = rid.slot_number;					
						
						//unpin the new leafPage created
						bufMgr->unPinPage(file, newPageId, true);
					} else {
						int index = findIndexIntoKeyArray(leafPage, keyPtr);
						
						//move the keys over on this page
						for(int i = leafOccupancy - 1; i > index; i--) {
							strncpy(leaf->keyArray[i], leaf->keyArray[i - 1], STRINGSIZE);
							leaf->ridArray[i].page_number = leaf->ridArray[i - 1].page_number;
							leaf->ridArray[i].slot_number = leaf->ridArray[i - 1].slot_number;
						}

						//actually insert the record
						strncpy(leaf->keyArray[index], key.c_str(), STRINGSIZE);
						leaf->ridArray[index].page_number = rid.page_number;
						leaf->ridArray[index].slot_number = rid.slot_number;
					}

				}
				//unpin the leaf page
				bufMgr->unPinPage(file, leafPageId, true);
			}
			
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
                else if(leaf->keyArray[i] == key) throw DuplicateKeyException();
				else if(key > leaf->keyArray[i] && i != leafOccupancy - 1 && key < leaf->keyArray[i + 1]) return i + 1;
				else if(key > leaf->keyArray[i] && (i == leafOccupancy - 1 || leaf->keyArray[i + 1] == INT_MAX)) return i + 1;
			}
			break;
		}
		case DOUBLE: {
		    double key = *((double*) keyPtr);
			LeafNodeDouble* leaf = (LeafNodeDouble*) page;
			for(int i = 0; i < leafOccupancy; i++) {
				if(leaf->keyArray[0] == DBL_MAX || key < leaf->keyArray[0]) return 0;
                else if(leaf->keyArray[i] == key) throw DuplicateKeyException();
				else if(key > leaf->keyArray[i] && i != leafOccupancy - 1 && key < leaf->keyArray[i + 1]) return i + 1;
				else if(key > leaf->keyArray[i] && (i == leafOccupancy - 1 || leaf->keyArray[i + 1] == DBL_MAX)) return i + 1;
			}
			break;
		}
		case STRING: {
			std::string key = *((std::string*) keyPtr);
			LeafNodeString* leaf = (LeafNodeString*) page;
			for(int i = 0; i < leafOccupancy; i++) {
				if(strcmp(leaf->keyArray[0], "") == 0 || strcmp(key.c_str(), leaf->keyArray[0]) < 0) return 0;
                else if(strcmp(key.c_str(), leaf->keyArray[i]) == 0) throw DuplicateKeyException();
				else if(strcmp(key.c_str(), leaf->keyArray[i]) > 0 && i != leafOccupancy - 1 && strcmp(key.c_str(), leaf->keyArray[i + 1]) < 0) return i + 1;
				else if(strcmp(key.c_str(), leaf->keyArray[i]) > 0 && (i == leafOccupancy - 1 || strcmp(leaf->keyArray[i + 1], "") == 0)) return i + 1;
			}
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
		    double key = *((double*) keyPtr);
			NonLeafNodeDouble* nodeDouble = (NonLeafNodeDouble*) page;

			for(int i = 0; i < nodeOccupancy; i++) {
				if(key < nodeDouble->keyArray[0]) {
					return 0; 
				}
				else if(key >= nodeDouble->keyArray[i] && !(i == nodeOccupancy - 1 || nodeDouble->keyArray[i+1] == DBL_MAX) && key < nodeDouble->keyArray[i + 1]) {
					return i + 1;
				}
				else if(key >= nodeDouble->keyArray[i] && (i == nodeOccupancy - 1 || nodeDouble->keyArray[i + 1] == DBL_MAX)) {
					return i + 1;
				}
			}
			break;
		}
		case STRING: {
			std::string key = *((std::string*) keyPtr);
			NonLeafNodeString* nodeString = (NonLeafNodeString*) page;

			for(int i = 0; i < nodeOccupancy; i++) {
				if(strcmp(key.c_str(), nodeString->keyArray[0]) < 0) {
					return 0; 
				}
				else if(strcmp(key.c_str(), nodeString->keyArray[i]) >= 0 && !(i == nodeOccupancy - 1 || strcmp(nodeString->keyArray[i+1], "") == 0) && strcmp(key.c_str(), nodeString->keyArray[i + 1]) < 0) {
					return i + 1;
				}
				else if(strcmp(key.c_str(), nodeString->keyArray[i]) >= 0 && (i == nodeOccupancy - 1 || strcmp(nodeString->keyArray[i + 1], "") == 0)) {
					return i + 1;
				}
			}
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
				LeafNodeDouble* leaf = (LeafNodeDouble*) page;
				double key = *((double*) keyPtr);
				if(leafOccupancy % 2 == 0) {
					if(key > leaf->keyArray[leafOccupancy/2 - 1] && key < leaf->keyArray[leafOccupancy/2]) {
						middleDouble = key;
						middleIndex = leafOccupancy/2;
					}
					else if(key > leaf->keyArray[leafOccupancy/2]) {
						middleDouble = (leaf->keyArray[leafOccupancy/2]);
						middleIndex = leafOccupancy/2;
					}
					else {
						middleDouble = (leaf->keyArray[leafOccupancy/2 - 1]);
						middleIndex = leafOccupancy/2 - 1;
					}
				} else {
					middleDouble = (leaf->keyArray[leafOccupancy/2]);
					middleIndex = leafOccupancy/2;
				}
			} else {
				NonLeafNodeDouble* node = (NonLeafNodeDouble*) page;
				double key = *((double*) keyPtr);
				if(nodeOccupancy % 2 == 0) {
					if(key > node->keyArray[nodeOccupancy/2 - 1] && key < node->keyArray[nodeOccupancy/2]) {
						middleDouble = key;
						middleIndex = nodeOccupancy/2 - 1;
					}
					else if(key > node->keyArray[nodeOccupancy/2]) {
						middleDouble = (node->keyArray[nodeOccupancy/2]);
						middleIndex = nodeOccupancy/2;
					}
					else if(key < node->keyArray[nodeOccupancy/2 - 1]){
						middleDouble = (node->keyArray[nodeOccupancy/2 - 1]);
						middleIndex = nodeOccupancy/2 - 1;
					} else {
						//ERROR it equals some key passed it. Too late to throw error here as the data is already in the database 
					}
				} else {
					if(key > node->keyArray[nodeOccupancy/2-1] && key < node->keyArray[nodeOccupancy/2]) {
						middleDouble = key;
						middleIndex = nodeOccupancy/2 - 1;
					} else if(key > node->keyArray[nodeOccupancy/2] && key < node->keyArray[nodeOccupancy/2 + 1]) {
						middleDouble = key;
						middleIndex = nodeOccupancy/2;
					} else if(key < node->keyArray[nodeOccupancy/2-1]) {
						middleDouble = (node->keyArray[nodeOccupancy/2-1]);
						middleIndex = nodeOccupancy/2 - 1;
					} else if(key > node->keyArray[nodeOccupancy/2]) {
						middleDouble = (node->keyArray[nodeOccupancy/2]);
						middleIndex = nodeOccupancy/2;
					} else {
						//ERROR
					}
				}
			}
			break;
		}
		case STRING: {
			if(isLeaf) {
				LeafNodeString* leaf = (LeafNodeString*) page;
				std::string key = *((std::string*) keyPtr);
				if(leafOccupancy % 2 == 0) {
					if(strcmp(key.c_str(), leaf->keyArray[leafOccupancy/2 - 1]) > 0 && strcmp(key.c_str(), leaf->keyArray[leafOccupancy/2]) < 0) {
						middleString.assign(key);
						middleIndex = leafOccupancy/2;
					}
					else if(strcmp(key.c_str(), leaf->keyArray[leafOccupancy/2]) > 0) {
						middleString.assign(leaf->keyArray[leafOccupancy/2]);
						middleIndex = leafOccupancy/2;
					}
					else {
						middleString.assign(leaf->keyArray[leafOccupancy/2 - 1]);
						middleIndex = leafOccupancy/2 - 1;
					}
				} else {
					middleString.assign(leaf->keyArray[leafOccupancy/2]);
					middleIndex = leafOccupancy/2;
				}
			} else {
				NonLeafNodeString* node = (NonLeafNodeString*) page;
				std::string key = *((std::string*) keyPtr);
				if(nodeOccupancy % 2 == 0) {
					if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2 - 1]) > 0 && strcmp(key.c_str(), node->keyArray[nodeOccupancy/2]) < 0) {
						middleString.assign(key);
						middleIndex = nodeOccupancy/2 - 1;
					}
					else if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2]) > 0) {
						middleString.assign(node->keyArray[nodeOccupancy/2]);
						middleIndex = nodeOccupancy/2;
					}
					else if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2 - 1]) < 0){
						middleString.assign(node->keyArray[nodeOccupancy/2 - 1]);
						middleIndex = nodeOccupancy/2 - 1;
					} else {
						//ERROR it equals some key passed it. Too late to throw error here as the data is already in the database 
					}
				} else {
					if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2-1]) > 0 && strcmp(key.c_str(), node->keyArray[nodeOccupancy/2]) < 0) {
						middleString.assign(key);
						middleIndex = nodeOccupancy/2 - 1;
					} else if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2]) > 0 && strcmp(key.c_str(), node->keyArray[nodeOccupancy/2 + 1]) < 0) {
						middleString.assign(key);
						middleIndex = nodeOccupancy/2;
					} else if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2-1]) < 0) {
						middleString.assign(node->keyArray[nodeOccupancy/2-1]);
						middleIndex = nodeOccupancy/2 - 1;
					} else if(strcmp(key.c_str(), node->keyArray[nodeOccupancy/2]) > 0) {
						middleString.assign(node->keyArray[nodeOccupancy/2]);
						middleIndex = nodeOccupancy/2;
					} else {
						//ERROR
					}
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
            if(pageLevel == 0) {
				//find the index based on the key pointer
				int index = findIndexIntoPageNoArray(page, keyPtr);

				//read in that page and traverse down
				Page* child;
				bufMgr->readPage(file, ((NonLeafNodeDouble*) page)->pageNoArray[index], child);
				traverse(child, ((NonLeafNodeDouble*) child)->level, keyPtr, leafId);

				//unpin the node page
				bufMgr->unPinPage(file, ((NonLeafNodeDouble*) page)->pageNoArray[index], false);
			} else {
				//page is one above the leaf level
				int index = findIndexIntoPageNoArray(page, keyPtr);
				leafId = ((NonLeafNodeDouble*) page)->pageNoArray[index];
			}
			break;
		}
		case STRING: {

            std::cout << *((std::string*) keyPtr) << std::endl;
			if(pageLevel == 0) {
				//find the index based on the key pointer
				int index = findIndexIntoPageNoArray(page, keyPtr);

				//read in that page and traverse down
				Page* child;
				bufMgr->readPage(file, ((NonLeafNodeString*) page)->pageNoArray[index], child);
				traverse(child, ((NonLeafNodeString*) child)->level, keyPtr, leafId);

				//unpin the node page
				bufMgr->unPinPage(file, ((NonLeafNodeString*) page)->pageNoArray[index], false);
			} else {
				//page is one above the leaf level
				int index = findIndexIntoPageNoArray(page, keyPtr);
				leafId = ((NonLeafNodeString*) page)->pageNoArray[index];
			}
			break;
			break;
		}
		default: { break; }
	}
}
}
