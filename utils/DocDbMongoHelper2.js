//---------------------------------------------------------------------------------------------------
// Code should run in strict mode wherever possible.
"use strict";
//---------------------------------------------------------------------------------------------------
if (typeof __ != "undefined") {
    // Use 'let' inside a block so that everything defined here is not visible to user script.
    let setupSystemFunctions = function() {
        if (typeof __.sys == "undefined") __.sys = new Object();
        setupSys.call(__.sys);

        function setupSys() {
            // Note: set enumerable to true, if you want the function to be enumerated as properties of the __ object.
            Object.defineProperty(this, "tablesBatchOperation", { enumerable: false, configurable: false, writable: false, value: tablesBatchOperation });
        }

        function tablesBatchOperation(documents, operations, etags) {
            if (!documents || !operations || !etags) 
            {
                throw new Error('One of the input arrays (operations, operationType, etags is undefined or null.');
            }

            if(documents.Count != operations.Count || documents.Count != etags.Count) throw new Error('Operations, operationtype, etags count are inconsistent.');

            var links = new Array();
            var operationCount = 0;
            var isOperationLastStep = false;
            var isReadAttempted = false;
            var isInsertAttempted = false;
            var docResource;
            var collection = getContext().getCollection();
            var collectionLink = collection.getAltLink();
            
            ProcessBatch();

            function ProcessBatch() {
                var document = documents[operationCount];
                var operation = operations[operationCount];
                var etag = etags[operationCount];

                if(operation == '0') // Insert
                {
                    isOperationLastStep = true;
                    if (!collection.createDocument(collectionLink, document, onInsertCompletion)) {
                        failedToEnqueueOperation();
                    } 
                }
                else if(operation == '1') // delete
                {
                    let documentLink = `${collectionLink}/docs/${document.id}`;
                    isOperationLastStep = true;
                    if(!collection.deleteDocument(documentLink, {etag: etag}, onDocumentActionCompletion)) {
                        failedToEnqueueOperation();
                    } 
                }
                else if(operation == '2') // replace
                {
                    let documentLink = `${collectionLink}/docs/${document.id}`;
                    isOperationLastStep = true;
                    if(!collection.replaceDocument(documentLink, document, {etag: etag}, onDocumentActionCompletion)) {
                        failedToEnqueueOperation();
                    } 
                }
                else if(operation == '3') // merge?
                {
                    let documentLink = `${collectionLink}/docs/${document.id}`;
                    if(!isReadAttempted)
                    {
                        if (!collection.readDocument(documentLink, {}, onReadCompletion)) {
                            failedToEnqueueOperation();
                        }
                    }
                    else
                    {
                        for (var propName of Object.getOwnPropertyNames(document))
                        {
                            docResource[propName] = document[propName];
                        }

                        isOperationLastStep = true;
                        if(!collection.replaceDocument(documentLink, docResource, 
                           {etag: etag}, onDocumentActionCompletion)) {
                            failedToEnqueueOperation();
                        } 
                    }
                }
                else if(operation == '4') // insertreplace - Upsert (create and/or replace)
                {
                    if(!isInsertAttempted)
                    {
                        if (!collection.createDocument(collectionLink, document, onInsertCompletion)) {
                            failedToEnqueueOperation();
                        }
                    }
                    else
                    {
                        let documentLink = `${collectionLink}/docs/${document.id}`;
                        isOperationLastStep = true;
                        if(!collection.replaceDocument(documentLink, document, {}, onDocumentActionCompletion)) {
                            failedToEnqueueOperation();
                        }
                    }
                }
                else if(operation == '5') // insertMerge - Read, Merge, Update
                {
                    let documentLink = `${collectionLink}/docs/${document.id}`;
                    if(!isInsertAttempted)
                    {
                        if (!collection.createDocument(collectionLink, document, onInsertCompletion)) {
                            failedToEnqueueOperation();
                        }
                    }
                    else if(!isReadAttempted)
                    {
                        if (!collection.readDocument(documentLink, {}, onReadCompletion)) {
                            failedToEnqueueOperation();
                        }
                    }
                    else
                    {
                        for (var propName of Object.getOwnPropertyNames(document))
                        {
                            docResource[propName] = document[propName];
                        }
                        
                        isOperationLastStep = true;
                        if(!collection.replaceDocument(documentLink, docResource, 
                           {}, onDocumentActionCompletion)) {
                            failedToEnqueueOperation();
                        } 
                    }
                }
                else if(operation == '6') // retrieve
                {
                    let documentLink = `${collectionLink}/docs/${document.id}`;
                    isOperationLastStep = true;
                    if (!collection.readDocument(documentLink, {}, onDocumentActionCompletion)) {
                        failedToEnqueueOperation();
                    } 
                }
            }

            function onReadCompletion(err, resource, responseOptions)
            {
                if (err)
                {
                    throwBatchOperationError(err);
                }

                if(isOperationLastStep)
                {
                    isOperationLastStep = false;
                    isReadAttempted = false;
                    operationCount++;
                                    
                    links.push(resource);
                }
                else
                {
                    isReadAttempted = true;
                    docResource = resource;
                }
                                
                moveNext();
            }
                            
            function onInsertCompletion(err, resource, responseOptions)
            {
                if(err)
                {
                    if(!isOperationLastStep && err.number == ErrorCodes.Conflict)
                    {
                        isInsertAttempted = true;
                    }
                    else
                    {
                        throwBatchOperationError(err);
                    }
                }
                else
                {
                    isOperationLastStep = false;
                    isInsertAttempted = false;
                    operationCount++;
                                    
                    links.push(resource);
                }
                                
                moveNext();
            }
                            
            function onDocumentActionCompletion(err, resource, responseOptions)
            {
                if (err)
                {
                    throwBatchOperationError(err);
                }

                if(isOperationLastStep)
                {
                    isInsertAttempted = false;
                    isReadAttempted = false;
                    isOperationLastStep = false;
                    operationCount++;
                                    
                    links.push(resource);
                }
                                
                moveNext();
            }

            function moveNext() {
                if (operationCount < documents.length) {
                    ProcessBatch();
                } else {
                    setResponseBody();
                }
            }
                            
            //unable to break batch into multiple continuations since transactional behavior is important.
            function failedToEnqueueOperation() {
                throw new Error('Failed to enqueue operation ' + operationCount);
            }

            function setResponseBody() {
                getContext().getResponse().setBody(JSON.stringify(links));
            }
            
            function throwBatchOperationError(err) {
                if(err)
                {
                    throw new Error('OperationCount:' + operationCount + '.\n' + JSON.stringify(err));
                }
                else
                {
                    throw new Error('Unknown error on operation ' + operationCount);
                }
            }
        }
    }

    setupSystemFunctions();
}
