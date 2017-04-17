import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class sparqlToSql {

	public final static Map<String, String> attributeMap = new HashMap<>();
	static {
		attributeMap.put("birthday", "生日");
		attributeMap.put("sex", "性别");
		attributeMap.put("birthPlace", "出生地");
		attributeMap.put("bloodType", "血型");
		attributeMap.put("age", "年龄");
		attributeMap.put("inHospitalNumber", "住院次数");
		attributeMap.put("assayInformation", "化验信息");
		attributeMap.put("diseaseInformation", "疾病信息");
		attributeMap.put("diseaseDetail", "疾病详情");
		attributeMap.put("diseaseName", "疾病名称");
		attributeMap.put("adviceInformation", "医嘱信息");
		attributeMap.put("operationInformation", "手术信息");
		attributeMap.put("hospitalInformation", "住院信息");
		attributeMap.put("hospitalNumber", "住院号");
		attributeMap.put("applyTime", "申请时间");
		attributeMap.put("resultTime", "结果时间");
		attributeMap.put("assayDetail", "化验详情");
		attributeMap.put("inHospitalTime", "入院日期");
		attributeMap.put("ioutHospitalTime", "出院日期");
	}

	// 参数分别为SPARQL语句路径，SQL路径，以及MySQL连接
	public void transform(String SparqlPath, String SQLPath, Connection connection) throws SQLException, IOException {

		try {
			BufferedReader sparqlReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(new File(SparqlPath)), "UTF-8"));
			BufferedWriter sqlWriter = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(new File(SQLPath)), "UTF-8"));
			String prefix = "", URI = "", SELECT = "", WHERE = "", predicate = "", pComplete = "", FILTER = "";
			String SQL_SELECT = "select ", SQL_WHERE = "where ", SQL_FROM = "from ";
			String[] triples = null;
			prefix = sparqlReader.readLine();
			URI = prefix.substring(prefix.indexOf('<') + 1, prefix.indexOf('>'));
			SELECT = sparqlReader.readLine();
			String oneLine = "";
			int i = 1;// attribute表别名后缀
			int EMPIFlag = 0;// 标识三元组模式中是否含有EMPI,有为1，没有是0
			int selectFlag = 0;// SELECT中是否是第一个attribute,0是，1不是

			while ((oneLine = sparqlReader.readLine()) != null) {
				if (oneLine.contains("where"))
					continue;
				if (oneLine.contains("filter")) {
					FILTER = oneLine;
					continue;
				}
				if (oneLine.contains("{") || oneLine.contains("}") || oneLine.contains("group"))
					continue;
				WHERE = WHERE + oneLine;
			}
			triples = WHERE.split("\\.");// 根据.将三元组模式进行分割

			for (int j = 0; j < triples.length; j++) {
				String triple = triples[j];
				if (triple.contains("EMPI")) {
					SQL_SELECT = SQL_SELECT + "distinct entity.name";
					SQL_FROM = SQL_FROM + "entity,attribute attribute_1";
					SQL_WHERE = SQL_WHERE
							+ "attribute_1.entity_id=entity.entity_id and attribute_1.entity_id=attribute_2.entity_id";
					EMPIFlag = 1;
					i++;
					continue;
				}
				String regEx = ":(.+?)\\b";
				Pattern pattern = Pattern.compile(regEx);
				Matcher matcher = pattern.matcher(triple);
				while (matcher.find()) {
					predicate = matcher.group(1);
					System.out.println(predicate);
				}
				// 取出属性中文名
				String predicateName = attributeMap.get(predicate);
				String SQL = "select id from attribute_definition where name=" + "'" + predicateName + "'";
				PreparedStatement pStatement = (PreparedStatement) connection.prepareStatement(SQL);
				ResultSet rSet = pStatement.executeQuery();
				int ID = 0;
				// 取出属性对应的ID
				if (rSet.wasNull())
					System.out.println("connot find the attribute!");

				while (rSet.next()) {
					ID = rSet.getInt(1);
					if (EMPIFlag == 1)
						SQL_FROM = SQL_FROM + ",attribute attribute_" + i;
					else {
						switch (selectFlag) {
						case 0:
							SQL_FROM = SQL_FROM + "attribute attribute_" + i;
							selectFlag = 1;
							break;
						case 1:
							SQL_FROM = SQL_FROM + ",attribute attribute_" + i;
							break;
						default:
							break;
						}
					}
				}

				// 判断三元组模式是<?s p ?o>还是<?s p o>
				// 两个?，是<?s p ?o>
				if ((triple.length() - triple.replace("?", "").length()) == 2) {
					if (j + 1 < triples.length) {
						// 如果此三元组模式的?o和下一条三元组模式的?s相同
						if (triple.split(" ")[2].equals(triples[j + 1].split(" ")[0])) {
							if (EMPIFlag == 1) {
								SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_id=" + ID + " and attribute_" + i
										+ ".attr_value=attribute_" + (i + 1) + ".entity_id";
							}

							if (EMPIFlag == 0) {
								SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_id=" + ID + " and attribute_" + i
										+ ".attr_value=attribute_" + (i + 1) + ".entity_id";
							}
							i++;
						}
					} else if (j + 1 == triples.length) {
						// 后接FILTER语句
						if (FILTER.length() > 0) {
							// FILTER中为时间限制
							if (FILTER.contains("xsd:date")) {
								String[] items = FILTER.substring(FILTER.indexOf("(") + 1, FILTER.indexOf(")")).trim()
										.split("&&");
								int attrFlag = 0;// 标识该属性是否出现过
								for (String item : items) {
									// 判断大于号还是小于号
									if (item.contains("<")) {
										switch (attrFlag) {
										case 0:
											String time = item.substring(item.indexOf("\"") + 1,
													item.lastIndexOf("\""));
											SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_id=" + ID
													+ " and attribute_" + i + ".attr_value<'" + time + "'";
											attrFlag = 1;
											break;
										case 1:
											String time1 = item.substring(item.indexOf("\"") + 1,
													item.lastIndexOf("\""));
											SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_value<'" + time1
													+ "'";
											attrFlag = 1;
											break;
										default:
											break;
										}
									}
									if (item.contains(">")) {
										switch (attrFlag) {
										case 0:
											String time = item.substring(item.indexOf("\"") + 1,
													item.lastIndexOf("\""));
											SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_id=" + ID
													+ " and attribute_" + i + ".attr_value>'" + time + "'";
											attrFlag = 1;
											break;
										case 1:
											String time1 = item.substring(item.indexOf("\"") + 1,
													item.lastIndexOf("\""));
											SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_value>'" + time1
													+ "'";
											attrFlag = 1;
											break;
										default:
											break;
										}
									}
								}
							}
						} else {
							SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_id=" + ID;// 最后一个三元组模式的属性
						}
					}
				}
				// 只有一个?，是<?s p o>
				else if ((triple.length() - triple.replace("?", "").length()) == 1) {
					if (EMPIFlag == 1) {
						SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".attr_id=" + ID + " and attribute_" + i
								+ ".attr_value=" + triple.split(" ")[2].replace("\"", "'");// 将属性值中的"换成'，因为SQL不支持"
						if (j + 1 < triples.length)
							SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".entity_id=attribute_" + (i + 1)
									+ ".entity_id";// 此三元组模式的entity_id和下一条三元组模式的entity_id相同
					}

					if (EMPIFlag == 0) {
						SQL_WHERE = SQL_WHERE + "attribute_" + i + ".attr_id=" + ID + " and attribute_" + i
								+ ".attr_value=" + triple.split(" ")[2].replace("\"", "'");// 将属性值中的"换成'，因为SQL不支持"
						SQL_WHERE = SQL_WHERE + " and attribute_" + i + ".entity_id=attribute_" + (i + 1)
								+ ".entity_id";// 此三元组模式的entity_id和下一条三元组模式的entity_id相同
					}
					i++;
				}
			}

			// 处理SELECT子句
			SELECT = SELECT.substring(SELECT.indexOf("?"), SELECT.length());
			String[] select = null;
			String aggregate = "";// 聚合函数名称
			String regEx = "\\((.+?)\\(";
			Pattern pattern = Pattern.compile(regEx);
			Matcher matcher = pattern.matcher(SELECT);
			while (matcher.find()) {
				aggregate = matcher.group(1);
				System.out.println(aggregate);
			}
			// EMPI存在的时候只会是min、max、avg聚合函数
			if (EMPIFlag == 1) {
				// 如果有聚合函数
				if (aggregate.length() > 0) {
					SQL_SELECT = SQL_SELECT + "," + aggregate + "(attribute_" + i + ".attr_value)";
				}
			} else {
				// EMPI不存在的时候聚合函数为count
				if (aggregate.length() > 0) {
					SQL_SELECT = SQL_SELECT + "attribute_" + i + ".attr_value," + aggregate
							+ "(attribute_1.entity_id) as number";
				}
			}
			sqlWriter.write(SQL_SELECT + "\n");
			sqlWriter.write(SQL_FROM + "\n");
			sqlWriter.write(SQL_WHERE + "\n");
			sqlWriter.close();
			sparqlReader.close();
			System.out.println(SQL_SELECT);
			System.out.println(SQL_FROM);
			System.out.println(SQL_WHERE);

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
