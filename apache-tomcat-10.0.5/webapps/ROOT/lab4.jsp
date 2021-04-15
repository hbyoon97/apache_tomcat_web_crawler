<html><head><meta http-equiv="Content-Type" content="text/html; charset=windows-1252"></head><body>
The words you entered are: <br>
<%
String s = request.getParameter("txtname");
if(s != null) {
	String[] words = s.split(" ");
	for (String w : words) {
		out.print(w + "<br>");
	}
}

%>


</body></html>
