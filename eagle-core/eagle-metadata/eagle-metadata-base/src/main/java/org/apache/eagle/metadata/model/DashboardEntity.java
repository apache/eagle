/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.model;

import com.google.common.base.Preconditions;
import org.apache.eagle.metadata.persistence.PersistenceEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dashboard Config Entity.
 */
public class DashboardEntity extends PersistenceEntity {

    private String name;
    private String description;
    private String author;
    /**
     * Dashboard Level Settings.
     */
    private Map<String,Object> settings;

    /**
     * Charts configurations.
     */
    private List<String> charts;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings;
    }

    public List<String> getCharts() {
        return charts;
    }

    public void setCharts(List<String> charts) {
        this.charts = charts;
    }

    @Override
    public void ensureDefault() {
        Preconditions.checkNotNull(this.name, "name should not be null");
        Preconditions.checkNotNull(this.description, "description should not be null");
        Preconditions.checkNotNull(this.author, "author should not be null");
        if (this.settings == null) {
            this.settings = new HashMap<>();
        }
        if (this.charts == null) {
            this.charts = new ArrayList<>(0);
        }
        super.ensureDefault();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DashboardEntity entity = (DashboardEntity) o;

        if (name != null ? !name.equals(entity.name) : entity.name != null) {
            return false;
        }
        if (description != null ? !description.equals(entity.description) : entity.description != null) {
            return false;
        }
        if (author != null ? !author.equals(entity.author) : entity.author != null) {
            return false;
        }
        if (settings != null ? !settings.equals(entity.settings) : entity.settings != null) {
            return false;
        }
        return charts != null ? charts.equals(entity.charts) : entity.charts == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (author != null ? author.hashCode() : 0);
        result = 31 * result + (settings != null ? settings.hashCode() : 0);
        result = 31 * result + (charts != null ? charts.hashCode() : 0);
        return result;
    }
}