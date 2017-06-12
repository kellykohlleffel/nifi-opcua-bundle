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
package com.kentender.nifi.nifi_opcua_services;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.opcfoundation.ua.builtintypes.ExpandedNodeId;
import org.opcfoundation.ua.builtintypes.UnsignedInteger;
import java.util.List;

@Tags({"OPC Service API"})
@CapabilityDescription("Provides client API for working with OPC servers")
public interface OPCUAService extends ControllerService {

	byte[] getValue(List<String> reqTagname, String returnTimestamp, String excludeNullValue, String nullValueString) throws ProcessException;
	
	String getNameSpace(String print_indentation, int max_recursiveDepth, List<ExpandedNodeId> expandedNodeIds, UnsignedInteger max_reference_per_node)
			throws ProcessException;

	boolean updateSession();

	String getCurrentTagList();

	void setCurrentTagList(String currentTagList);
}
