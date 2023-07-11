/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.plugin.datasource.dialect;

import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.plugin.datasource.constants.DatabaseTypeConstant;

/***
 * oracle datasource dialect.
 * @author onewe
 */
public class OracleDatabaseDialect extends AbstractDatabaseDialect {

    private static final String DEFAULT_NAMESPACE_ID = "public";

    static {
        NamespaceUtil.namespaceDefaultId = DEFAULT_NAMESPACE_ID;
    }

    @Override
    public String getType() {
        return DatabaseTypeConstant.ORACLE;
    }

    @Override
    public String getLimitTopSqlWithMark(String sql) {
        return sql + " AND ROWNUM < ? ";
    }

    @Override
    public String getLimitPageSqlWithMark(String sql) {
        return "SELECT * FROM (  SELECT TMP_PAGE.*, ROWNUM PAGEHELPER_ROW_ID FROM ( " + sql
                + ") TMP_PAGE) WHERE PAGEHELPER_ROW_ID <= ? AND PAGEHELPER_ROW_ID > ? ";


//		return sql + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY ";
    }

    @Override
    public String getLimitPageSqlWithOffset(String sql, int startOffset, int pageSize) {
        return "SELECT * FROM (  SELECT TMP_PAGE.*, ROWNUM PAGEHELPER_ROW_ID FROM ( " + sql
                + ") TMP_PAGE) WHERE PAGEHELPER_ROW_ID <= " + startOffset + pageSize + " AND PAGEHELPER_ROW_ID >  " + startOffset;
//		return sql + "  OFFSET " + startOffset + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY ";
    }

    @Override
    public String getLimitPageSql(String sql, int pageNo, int pageSize) {
        return "SELECT * FROM (  SELECT TMP_PAGE.*, ROWNUM PAGEHELPER_ROW_ID FROM ( " + sql
                + ") TMP_PAGE) WHERE PAGEHELPER_ROW_ID <= " + pageSize*pageNo  + " AND PAGEHELPER_ROW_ID >  " + getPagePrevNum(pageNo, pageSize);

//        return sql + "  OFFSET " + getPagePrevNum(pageNo, pageSize) + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY ";
    }
}
