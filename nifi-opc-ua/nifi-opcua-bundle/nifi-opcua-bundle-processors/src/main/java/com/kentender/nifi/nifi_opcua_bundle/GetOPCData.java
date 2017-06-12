/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kentender.nifi.nifi_opcua_bundle;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.kentender.nifi.nifi_opcua_services.OPCUAService;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"OPC", "OPCUA", "UA"})
@CapabilityDescription("Fetches a response from an OPC UA server based on configured name space and input item names")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
//@InputRequirement(Requirement.INPUT_REQUIRED)

public class GetOPCData extends AbstractProcessor {

    private final AtomicReference<String> timestamp = new AtomicReference<>();
    private final AtomicReference<String> excludeNullValue = new AtomicReference<>();
    private String nullValueString = "";

    private ComponentLog logger;
    private String query = "";

    public GetOPCData(){

    }

	public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
			  .name("OPC UA Service")
			  .description("Specifies the OPC UA Service that can be used to access data")
			  .required(true)
			  .identifiesControllerService(OPCUAService.class)
              .sensitive(false)
			  .build();
    
	public static final PropertyDescriptor RETURN_TIMESTAMP = new PropertyDescriptor
            .Builder().name("Return Timestamp")
            .description("Allows to select the source, server, or both timestamps")
            .required(true)
            .sensitive(false)
            .allowableValues("SourceTimestamp", "ServerTimestamp","Both")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TAG_LIST_SOURCE = new PropertyDescriptor
            .Builder().name("Tag List Source")
            .description("Either get the tag list from the flow file, or from a dynamic property")
            .required(true)
            .allowableValues("Flowfile", "Property")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TAG_LIST_FILE = new PropertyDescriptor
            .Builder().name("Default Tag List Name")
            .description("The location of the tag list file")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .sensitive(false)
            .build();
	 
	public static final PropertyDescriptor EXCLUDE_NULL_VALUE = new PropertyDescriptor
            .Builder().name("Exclude Null Value")
            .description("Return data only for non null values")
            .required(true)
            .sensitive(false)
            .allowableValues("No", "Yes")
            .defaultValue("No")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor NULL_VALUE_STRING = new PropertyDescriptor
            .Builder().name("Null Value String")
            .description("If removing null values, what string is used for null")
            .required(false)
            .sensitive(false)
            .build();
	 
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC Data read")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC Data read")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(RETURN_TIMESTAMP);
        descriptors.add(EXCLUDE_NULL_VALUE);
        descriptors.add(NULL_VALUE_STRING);
        descriptors.add(TAG_LIST_FILE);
        descriptors.add(TAG_LIST_SOURCE);
        
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        logger = getLogger();
        timestamp.set(context.getProperty(RETURN_TIMESTAMP).getValue());
        excludeNullValue.set(context.getProperty(EXCLUDE_NULL_VALUE).getValue());
        if (context.getProperty(NULL_VALUE_STRING).isSet()) {
            nullValueString = context.getProperty(NULL_VALUE_STRING).getValue();
        }
        if (context.getProperty(TAG_LIST_SOURCE).toString().equals("Property") && query.equals(""))
        {
            try {
                query = parseFile(Paths.get(context.getProperty(TAG_LIST_FILE).toString()));
            } catch (IOException e) {
                logger.error("Error reading tag list from Property.");
            }
        }
	}

    /* (non-Javadoc)
     * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        logger = getLogger();

    	// Initialize  response variable
        final AtomicReference<List<String>> requestedTagnames = new AtomicReference<>();
        // Submit to getValue
        OPCUAService opcUAService;

        try {
            opcUAService = context.getProperty(OPCUA_SERVICE)
                    .asControllerService(OPCUAService.class);
        }
        catch (Exception ex){
            logger.error(ex.getMessage());
            return;
        }
        if (context.getProperty(TAG_LIST_SOURCE).toString().equals("Flowfile")) {

            // get FlowFile
            FlowFile flowFile = session.get();
            if (flowFile == null)
                return;

            // Read tag name from flow file content
            session.read(flowFile, in -> {

                try {
                    List<String> tagname = new BufferedReader(new InputStreamReader(in))
                            .lines().collect(Collectors.toList());

                    requestedTagnames.set(tagname);

                } catch (Exception e) {
                    logger.error("Failed to read flowfile " + e.getMessage());
                }

            });
        }
        else
        {
            try {
                if (query.equals(""))
                {
                    try {
                        query = parseFile(Paths.get(context.getProperty(TAG_LIST_FILE).toString()));
                    } catch (IOException e) {
                        logger.error("Error reading tag list from Property.");
                    }
                }
                if (query.equals(""))
                {
                    logger.error("Tag list " + opcUAService.getCurrentTagList() + " not registered in the tag list registry correctly.");
                    return;
                }
                List<String> tagNamesList = new BufferedReader(new StringReader(query)).lines().collect(Collectors.toList());
                requestedTagnames.set(tagNamesList);
            }
            catch (Exception ex){
                logger.error(ex.getMessage());
                return;
            }
        }
        



        if(opcUAService.updateSession()){
        	logger.debug("Session current");
        }else {
        	logger.debug("Session update failed");
        }

        byte[] values = opcUAService.getValue(requestedTagnames.get(),timestamp.get(),excludeNullValue.get(),nullValueString);
        FlowFile flowFile;
        flowFile = session.get();
        if (flowFile == null)
            flowFile = session.create();
  		// Write the results back out to flow file
        try{

            flowFile = session.write(flowFile, out -> out.write(values));
        
        session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
        	logger.error("Unable to process", ex);
        	session.transfer(flowFile, FAILURE);
        }
    }

    private String parseFile(Path filePath) throws IOException
    {
        byte[] encoded;
        encoded = Files.readAllBytes(filePath);
        return new String(encoded, Charset.defaultCharset());
    }
}