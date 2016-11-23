package org.geotools.data.phoenix;

import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;

/**
 * 文件描述：Phoenix specific filter encoder.
 * 创建作者：陈苗
 * 创建时间：2016/11/1 10:47
 */
public class PhoenixFilterToSQL extends FilterToSQL {
    /**
     * 添加Phoenix所支持的空间函数
     * @return
     */
    @Override
    protected FilterCapabilities createFilterCapabilities() {
        /*添加函数*/
        return super.createFilterCapabilities();
    }
}
