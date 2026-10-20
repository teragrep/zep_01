package com.teragrep.zep_01.socket.fakes;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.*;

/**
 * Fake implementation of HttpServletRequest for use in tests. Returns empty primitives or throws a UnsupportedOperationException if trying to access methods that return more complicated objects.
 */

public class FakeHttpServletRequest implements HttpServletRequest {

    @Override
    public String getAuthType() {
        return "";
    }

    @Override
    public Cookie[] getCookies() {
        return new Cookie[0];
    }

    @Override
    public long getDateHeader(final String name) {
        return 0;
    }

    @Override
    public String getHeader(final String name) {
        return "";
    }

    @Override
    public Enumeration<String> getHeaders(final String name) {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public Enumeration<String> getHeaderNames() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public int getIntHeader(final String name) {
        return 0;
    }

    @Override
    public String getMethod() {
        return "";
    }

    @Override
    public String getPathInfo() {
        return "";
    }

    @Override
    public String getPathTranslated() {
        return "";
    }

    @Override
    public String getContextPath() {
        return "";
    }

    @Override
    public String getQueryString() {
        return "";
    }

    @Override
    public String getRemoteUser() {
        return "";
    }

    @Override
    public boolean isUserInRole(final String role) {
        return false;
    }

    @Override
    public Principal getUserPrincipal() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String getRequestedSessionId() {
        return "";
    }

    @Override
    public String getRequestURI() {
        return "";
    }

    @Override
    public StringBuffer getRequestURL() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String getServletPath() {
        return "";
    }

    @Override
    public HttpSession getSession(final boolean create) {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public HttpSession getSession() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String changeSessionId() {
        return "";
    }

    @Override
    public boolean isRequestedSessionIdValid() {
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromCookie() {
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromURL() {
        return false;
    }

    @Override
    public boolean isRequestedSessionIdFromUrl() {
        return false;
    }

    @Override
    public boolean authenticate(final HttpServletResponse response) throws IOException, ServletException {
        return false;
    }

    @Override
    public void login(final String username, final String password) throws ServletException {
       
    }

    @Override
    public void logout() throws ServletException {
       
    }

    @Override
    public Collection<Part> getParts() throws IOException, ServletException {
        return new ArrayList();
    }

    @Override
    public Part getPart(final String name) throws IOException, ServletException {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public <T extends HttpUpgradeHandler> T upgrade(final Class<T> handlerClass) throws IOException, ServletException {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public Object getAttribute(final String name) {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String getCharacterEncoding() {
        return "";
    }

    @Override
    public void setCharacterEncoding(final String env) throws UnsupportedEncodingException {
       
    }

    @Override
    public int getContentLength() {
        return 0;
    }

    @Override
    public long getContentLengthLong() {
        return 0;
    }

    @Override
    public String getContentType() {
        return "";
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String getParameter(final String name) {
        return "";
    }

    @Override
    public Enumeration<String> getParameterNames() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String[] getParameterValues(final String name) {
        return new String[0];
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        return new HashMap<>();
    }

    @Override
    public String getProtocol() {
        return "";
    }

    @Override
    public String getScheme() {
        return "";
    }

    @Override
    public String getServerName() {
        return "";
    }

    @Override
    public int getServerPort() {
        return 0;
    }

    @Override
    public BufferedReader getReader() throws IOException {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String getRemoteAddr() {
        return "";
    }

    @Override
    public String getRemoteHost() {
        return "";
    }

    @Override
    public void setAttribute(final String name, final Object o) {
       
    }

    @Override
    public void removeAttribute(final String name) {
       
    }

    @Override
    public Locale getLocale() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public Enumeration<Locale> getLocales() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public RequestDispatcher getRequestDispatcher(final String path) {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public String getRealPath(final String path) {
        return "";
    }

    @Override
    public int getRemotePort() {
        return 0;
    }

    @Override
    public String getLocalName() {
        return "";
    }

    @Override
    public String getLocalAddr() {
        return "";
    }

    @Override
    public int getLocalPort() {
        return 0;
    }

    @Override
    public ServletContext getServletContext() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public AsyncContext startAsync() throws IllegalStateException {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public AsyncContext startAsync(final ServletRequest servletRequest, final ServletResponse servletResponse) throws IllegalStateException {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public boolean isAsyncStarted() {
        return false;
    }

    @Override
    public boolean isAsyncSupported() {
        return false;
    }

    @Override
    public AsyncContext getAsyncContext() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }

    @Override
    public DispatcherType getDispatcherType() {
        throw new UnsupportedOperationException("Fake does not implement this!");
    }
}
