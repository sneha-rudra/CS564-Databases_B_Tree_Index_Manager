/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "file_iterator.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    //create the filename
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();
	
	//set values of the private variables
	bufMgr = bufMgrIn;
    attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
	scanExecuting = false;
	
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
		std::cout << "ERROR: non valid data type passed to BTreeIndex constructor\n";
	}

    //check if that file exists
	Page* metadataPage;
	IndexMetaInfo* metadata;
    try {
		file = new BlobFile(outIndexName, false);
	
		//if the file exists, read the first page which contains metadata information
		bufMgr->readPage(file, 1, metadataPage);
		metadata = (IndexMetaInfo*) metadataPage;
		
		//set the metadata for this file. We aren't supposed to throw exceptions in the
		//constructor so just overwrite whatever was there before
		metadata->attrType = attrType;
		metadata->attrByteOffset = attrByteOffset;
		strncpy(metadata->relationName, relationName.c_str(), 20);
		
		//set the root page for this index
		//FIXME: this is probably wrong?
		rootPageNum = metadata->rootPageNo;
		return;
	} catch(FileExistsException &e) {
		
	}
	
	//if the code reaches here then the file didnt exist so we must create one
	file = new BlobFile(outIndexName, true);
	
	//make a metadata Page for this new index
	PageId metadataPageId;
	bufMgr->allocPage(file, metadataPageId, metadataPage);
	bufMgr->readPage(file, metadataPageId, metadataPage);
	metadata = (IndexMetaInfo*) metadataPage;
	
	//set variables in the metadata page
	strncpy(metadata->relationName, relationName.c_str(), 20);
	metadata->attrType = attrType;
	metadata->attrByteOffset = attrByteOffset;
	
	//create a new root page
	Page* rootPage;
	bufMgr->allocPage(file, rootPageNum /*private variable in this class. set here*/, rootPage);
	bufMgr->readPage(file, rootPageNum, rootPage);
	metadata->rootPageNo = rootPageNum;
	
	//the rootPage will become a leaf node depending on the attrType
	switch(attrType) {
		case INTEGER:
			
			break;
		case DOUBLE:
		
			break;
		case STRING:
		
			break;
		default: ;
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
					insertEntry((int*) (recordPtr + attrByteOffset), rid);
					break;
				case DOUBLE:
					insertEntry((double*) (recordPtr + attrByteOffset), rid);
					break;
				case STRING: 
					char keyBuf[10];
					strncpy(keyBuf, (char*)(recordPtr + attrByteOffset), sizeof(keyBuf));
					key = std::string(keyBuf);
					insertEntry(&key, rid);
					break;
				default: ;
			}
		}
	} catch (EndOfFileException &e) {
	
	}
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
	if (scanExecuting)
	{
		try {
			endScan();
		}
		catch (ScanNotInitializedException e) { std::cout << "ScanNotInitializedException thrown in BtreeIndex destructor"; }
		catch (PageNotPinnedException e) { std::cout << "PageNotPinned Exception thrown in BTreeIndex destructor"; }
				
	}
	
	// Flushing the index file rom the buffer manager (by calling bufMgr->flushFile()) f
	bufMgr->flushFile(file);
	
	// Deleting the file object instance. This automatically invokes the destructor of the File class and closes the index file.
	delete file;
	
	
	
	// TODO: Remember to clean up any state variables. Maybe state variables that we set up in the constructor? 
	// NOTE: The ~FileScan method (which also shuts down scan and unpins any pinned pages) sets the currentPage to null, clears the dirty bit and sets the file iterator 
	// to point to the beginning of the file. Not sure if that's what they mean by perform any cleanup. This is not done in any other version though.
	
	
	// NOTE: Not using 'PageNotPinnedException& e' based on syntax in main.cpp test4 of Project3. We can change this if that's more appropriate.
	// NOTE: Printng out error messages for now. We ca remove them if required.
	// NOTE: catching the PageNotPinnedException here. We can catch it in endScan if that seems more appropriate.
	
	
	// QUESTION: Can both the exception objects have the same name 'e'?
	// QUESTION: One implementation flushFile method only if the file exists. The other one flushes it anyway. Not sure what we prefer?
	
	// MINOR QUESTION: flushFile should have its input as a const File* but file is not of 'const' type does that matter? 
	// Same question also for second argument of unPinPage
	
	

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
      
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

   // Method terminates the current scan and  throws a ScanNotInitializedException if invoked before a succesful startScan call
   if(!scanExecuting){
        throw ScanNotInitializedException();
   }
   else
   {    
        // Resetting scan specific variables
        scanExecuting = false;
   }

   // Unpinning all the pages that have been pinned for the purpose of scan
   bufMgr->unPinPage(file, currentPageNum, false);
   
   // TODO: Should we catch PageNotPinned exception? If yes, then here or in the destructor? Catching it in the destructor for now.
   // TODO: Header file says this method should reset scan specific variables. Which other variables might we need to reset here?   
   // TODO: Should the dirty bit be set to false irrespective of whether the page is actually dirty or not?
   // TODO: Check if the currentPage is the only page pinned for the purpose of the scan 
   // QUESTION: currentPageNum is not a 'const' pageId object. Does that matter?
   // QUESTION: When FileScan (used to scan records in a file) ends the scan (in its destructor), it sets the currentPage to null, its dirty bit to null, sets the file iteratir to point to the
   // beginning of the file and closes the file object. While shutting down a filtered scan of the index we dont need to do any of this?
    
   // QUESTION: Inside of ~FileScan (which also unpins any pinned pages), they first check if the currPage =! null and only then they call 
   // the unpinPage method. Should we do that? 
		

}

}
