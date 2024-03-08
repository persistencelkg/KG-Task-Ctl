package org.kg.ctl.config;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.injector.AbstractMethod;
import com.baomidou.mybatisplus.core.injector.DefaultSqlInjector;
import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.core.toolkit.sql.SqlScriptUtils;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.kg.ctl.core.AbstractTaskFromTo;
import org.kg.ctl.service.TableMetaData;
import org.kg.ctl.util.TaskUtil;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Description:
 * Author: 李开广
 * Date: 2024/3/4 7:43 PM
 */
@Component
@AllArgsConstructor
public class CustomSqlInjector extends DefaultSqlInjector {

    private BeanFactory beanFactory;

    @Override
    public List<AbstractMethod> getMethodList(Class<?> mapperClass) {
        List<AbstractMethod> methodList = super.getMethodList(mapperClass);
        Predicate<TableInfo> predicate = predicate();
        methodList.add(new UpdateWithClone(predicate));
        methodList.add(new InsertWithClone(predicate));
        return methodList;
    }

    public Predicate<TableInfo> predicate() {
        ResolvableType resolvableType = ResolvableType.forClass(AbstractTaskFromTo.class);
        ResolvableType[] generics = resolvableType.getGenerics();
        if (generics.length != 2) {
            return ref -> true;
        }
        Class<?> toClass = generics[1].resolve();
        if (Objects.isNull(toClass)) {
            return ref -> true;
        }
        String simpleName = toClass.getSimpleName();
        String underLine = TaskUtil.camelToUnderLine(simpleName);
        TableMetaData bean = ((TableMetaData) beanFactory.getBean(toClass));
        return ref -> !bean.updateInsertSetId() && underLine.startsWith(ref.getTableName());
    }

    @NoArgsConstructor
    @AllArgsConstructor
    private static class UpdateWithClone extends AbstractMethod {

        /**
         * 字段筛选条件
         */
        @Setter
        @Accessors(chain = true)
        private Predicate<TableFieldInfo> predicate;

        private Predicate<TableInfo> tableInfoPredicate;

        public UpdateWithClone(Predicate<TableInfo> tableInfoPredicate) {
            this.tableInfoPredicate = tableInfoPredicate;
        }


        @Override
        public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
            SqlMethod sqlMethod = SqlMethod.UPDATE_BY_ID;
            final String additional = optlockVersion() + tableInfo.getLogicDeleteSql(true, true);
            String sqlSet = this.filterTableFieldInfo(tableInfo.getFieldList(), getPredicate(tableInfo),
                    i -> i.getSqlSet(true, ENTITY_DOT), NEWLINE);
            sqlSet = SqlScriptUtils.convertSet(sqlSet);
            String sql = String.format(sqlMethod.getSql(), tableInfo.getTableName(), sqlSet,
                    tableInfo.getKeyColumn(), ENTITY_DOT + tableInfo.getKeyProperty(), additional);
            SqlSource sqlSource = languageDriver.createSqlSource(configuration, sql, modelClass);
            return addUpdateMappedStatement(mapperClass, modelClass, getMethod(sqlMethod), sqlSource);
        }


        private Predicate<TableFieldInfo> getPredicate(TableInfo tableInfo) {
            Predicate<TableFieldInfo> noLogic = t -> !t.isLogicDelete();
            if (Objects.nonNull(tableInfoPredicate)) {
                if (tableInfoPredicate.test(tableInfo)) {
                    return noLogic.and(t -> !Objects.equals("id", t.getColumn()));
                }
            }
            if (predicate != null) {
                return noLogic.and(predicate);
            }
            return noLogic;
        }

        @Override
        public String getMethod(SqlMethod sqlMethod) {
            // 自定义 mapper 方法名
            return "updateWithClone";
        }
    }


    @NoArgsConstructor
    @AllArgsConstructor
    private static class InsertWithClone extends AbstractMethod {

        /**
         * 字段筛选条件
         */
        @Setter
        @Accessors(chain = true)
        private Predicate<TableFieldInfo> predicate;

        private Predicate<TableInfo> tableInfoPredicate;

        public InsertWithClone(Predicate<TableInfo> tableInfoPredicate) {
            this.tableInfoPredicate = tableInfoPredicate;
        }

        @SuppressWarnings("Duplicates")
        @Override
        public MappedStatement injectMappedStatement(Class<?> mapperClass, Class<?> modelClass, TableInfo tableInfo) {
            KeyGenerator keyGenerator = new NoKeyGenerator();
            SqlMethod sqlMethod = SqlMethod.INSERT_ONE;
            List<TableFieldInfo> fieldList = tableInfo.getFieldList();
            String insertSqlColumn = tableInfo.getKeyInsertSqlColumn(false) +
                    this.filterTableFieldInfo(fieldList, getPredicate(tableInfo), TableFieldInfo::getInsertSqlColumn, EMPTY);
            String columnScript = LEFT_BRACKET + insertSqlColumn.substring(0, insertSqlColumn.length() - 1) + RIGHT_BRACKET;
            String insertSqlProperty = tableInfo.getKeyInsertSqlProperty(ENTITY_DOT, false) +
                    this.filterTableFieldInfo(fieldList,  getPredicate(tableInfo), i -> i.getInsertSqlProperty(ENTITY_DOT), EMPTY);
            insertSqlProperty = LEFT_BRACKET + insertSqlProperty.substring(0, insertSqlProperty.length() - 1) + RIGHT_BRACKET;
            String valuesScript = SqlScriptUtils.convertForeach(insertSqlProperty, "list", null, ENTITY, COMMA);
            String keyProperty = null;
            String keyColumn = null;
            // 表包含主键处理逻辑,如果不包含主键当普通字段处理
            if (StringUtils.isNotBlank(tableInfo.getKeyProperty())) {
                if (tableInfo.getIdType() == IdType.AUTO) {
                    /* 自增主键 */
                    keyGenerator = new Jdbc3KeyGenerator();
                    keyProperty = tableInfo.getKeyProperty();
                    keyColumn = tableInfo.getKeyColumn();
                } else {
                    if (null != tableInfo.getKeySequence()) {
                        keyGenerator = TableInfoHelper.genKeyGenerator(getMethod(sqlMethod), tableInfo, builderAssistant);
                        keyProperty = tableInfo.getKeyProperty();
                        keyColumn = tableInfo.getKeyColumn();
                    }
                }
            }
            String sql = String.format(sqlMethod.getSql(), tableInfo.getTableName(), columnScript, valuesScript);
            SqlSource sqlSource = languageDriver.createSqlSource(configuration, sql, modelClass);
            return this.addInsertMappedStatement(mapperClass, modelClass, getMethod(sqlMethod), sqlSource, keyGenerator, keyProperty, keyColumn);
        }

        private Predicate<TableFieldInfo> getPredicate(TableInfo tableInfo) {
            Predicate<TableFieldInfo> noLogic = t -> true;
            if (Objects.nonNull(tableInfoPredicate)) {
                if (tableInfoPredicate.test(tableInfo)) {
                    return noLogic.and(t -> !Objects.equals("id", t.getColumn()));
                }
            }
            if (predicate != null) {
                return noLogic.and(predicate);
            }
            return noLogic;
        }

        @Override
        public String getMethod(SqlMethod sqlMethod) {
            // 自定义 mapper 方法名
            return "insertWithClone";
        }
    }
}
