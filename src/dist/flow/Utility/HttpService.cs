/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Web.Script.Serialization;

namespace rDSN.Tron.Utility
{
    public class HttpService
    {
        public void RegisterStaticFileDirectory(string virtualPath, string localRoot)
        {
            if (virtualPath.Last() == '/' && virtualPath.Length > 1)
            {
                virtualPath = virtualPath.Substring(0, virtualPath.Length - 1);
            }

            if (localRoot.Last() != '/' && localRoot.Last() != '\\')
            {
                localRoot += '\\';
            }

            RegisterHandler(virtualPath, (c, p, q) => StaticFileHandler(virtualPath, Path.GetFullPath(localRoot), c, p, q));
        }

        public delegate string WebApiRequestHandler(string subApi, string query, string requestObjectInJson);

        public delegate TResponse TypedWebApiHandler<in TRequest, out TResponse>(TRequest request);

        public delegate TResponse TypedVoidWebApiHandler<out TResponse>();

        public void RegisterWebApi(string apiPath, WebApiRequestHandler handler)
        {
            RegisterHandler(apiPath, (c, p, q) => WebApiCallback(handler, c, p, q));
        }

        public void RegisterTypedWebApi<TRequest, TResponse>(string apiPath, TypedWebApiHandler<TRequest, TResponse> handler)
        {
            RegisterHandler(apiPath, (c, p, q) => WebApiCallback(
                (api, query, requestObjectInJson) => 
                    {
                        var req = new JavaScriptSerializer().Deserialize<TRequest>(requestObjectInJson);
                        var resp = handler(req);
                        return new JavaScriptSerializer().Serialize(resp);
                    }, 
                c, p, q)
                );
        }

        public void RegisterTypedWebApi<TResponse>(string apiPath, TypedVoidWebApiHandler<TResponse> handler)
        {
            RegisterHandler(apiPath, (c, p, q) => WebApiCallback(
                (api, query, requestObjectInJson) =>
                {
                    var resp = handler();
                    return new JavaScriptSerializer().Serialize(resp);
                },
                c, p, q)
                );
        }

        public FlowErrorCode Start(int port)
        {
            if (_listener != null)
            {
                return FlowErrorCode.HttpServiceAlreadyStarted;
            }

            if (!HttpListener.IsSupported)
            {
                return FlowErrorCode.PlatformNotSupported;
            }

            _sortedHandlerArrary = _handlers.ToArray();

            _listener = new HttpListener();
            _listener.Prefixes.Add($@"http://+:{port}/");
            _listener.AuthenticationSchemes = AuthenticationSchemes.Anonymous;
            _listener.Start();
            _listener.BeginGetContext(ContextCallback, null);

            _port = port;

            Console.WriteLine($"Http service at url: {_listener.Prefixes.First()}");

            return FlowErrorCode.Success;
        }

        public void Stop()
        {
            if (_listener == null)
            {
                return;
            }

            _listener.Stop();
            _listener = null;
            _port = 0;
        }

        protected virtual void Http404NotFoundHandler(HttpListenerContext context, string path, string query)
        {
            context.Response.StatusCode = 404;
            var responseString = "<HTML><BODY>404 Not Found.</BODY></HTML>";
            WriteAndCloseResponse(responseString, context);
        }

        public static string MIMETypeFromFileExtension(string ext)
        {
            switch (ext)
            {
                case ".avi": return "video/x-msvideo";
                case ".css": return "text/css";
                case ".doc": return "application/msword";
                case ".gif": return "image/gif";
                case ".htm":
                case ".html": return "text/html";
                case ".jpg":
                case ".jpeg": return "image/jpeg";
                case ".js": return "application/x-javascript";
                case ".mp3": return "audio/mpeg";
                case ".png": return "image/png";
                case ".pdf": return "application/pdf";
                case ".ppt": return "application/vnd.ms-powerpoint";
                case ".zip": return "application/zip";
                case ".json":
                case ".ini":
                case ".thrift":
                case ".cs":
                case ".h":
                case ".cpp":
                case ".cc":
                case ".java":
                case ".php":
                case ".txt": return "text/plain";
                default: return "application/octet-stream";
            }
        }

        protected virtual void StaticFileHandler(string virtualPath, string localRoot, HttpListenerContext context, string subpath, string query)
        {
            if (query.Length > 0)
            {
                Trace.TraceWarning($"static file download does not support queries: {subpath}?{query}");
            }

            var localPath = (localRoot + subpath).Replace('/', '\\').Replace("\\\\", "\\");

            // TODO: cache service
            if (File.Exists(localPath))
            {
                context.Response.ContentType = MIMETypeFromFileExtension(Path.GetExtension(localPath));

                using (var fileStream = File.OpenRead(localPath))
                {
                    context.Response.ContentLength64 = fileStream.Length;
                    fileStream.CopyTo(context.Response.OutputStream);
                }
                context.Response.OutputStream.Close();
            }

            else if (Directory.Exists(localPath))
            {
                context.Response.ContentType = "text/html";
                var responseString = ListAllInDirectory(virtualPath, subpath, localPath);
                WriteAndCloseResponse(responseString, context);
            }

            // File not found
            else
            {
                context.Response.ContentType = "text/html";
                Http404NotFoundHandler(context, subpath, query);
            }
        }

        protected string ListAllInDirectory(string virtualPath, string subPath, string localDir)
        {
            var logicalDir = (virtualPath + "/" + subPath).Replace('\\', '/').Replace("//", "/");
            if (logicalDir.Last() != '/') logicalDir += "/";

            var builder = new CodeBuilder();
            builder.AppendLine("<html>");
            builder.AppendLine("<title>" + logicalDir + "</title>");
            builder.AppendLine("<body>");
            builder.AppendLine("<h2>" + logicalDir + "</h2><hr>");

            if (subPath.Length > 0)
            {
                builder.AppendLine("<a href=\"" + logicalDir + "../\">..</a><br>");
            }

            foreach (var subdir in Directory.GetDirectories(localDir))
            {
                var dirname = Path.GetFileName(subdir);
                builder.AppendLine("<a href=\"" + logicalDir + dirname + "/\">" + dirname + "</a><br>");
            }

            foreach (var file in Directory.GetFiles(localDir))
            {
                var fileName = Path.GetFileName(file);
                builder.AppendLine("<a href=\"" + logicalDir + fileName + "\">" + fileName + "</a><br>");
            }

            builder.AppendLine("</body>");
            builder.AppendLine("</html>");
            return builder.ToString();
        }

        protected virtual void WebApiCallback(WebApiRequestHandler apiHandler, HttpListenerContext context, string subpath, string query)
        {
            context.Response.ContentType = "text/plain";

            var requestStr = "";
            var request = context.Request;
            if (request.HasEntityBody)
            {
                using (var body = request.InputStream) // here we have data
                {
                    using (var reader = new StreamReader(body, request.ContentEncoding))
                    {
                        requestStr = reader.ReadToEnd();
                    }
                }
            }
            
            WriteAndCloseResponse(apiHandler(subpath, query, requestStr), context);
        }

        private void WriteAndCloseResponse(string response, HttpListenerContext context)
        {
            context.Response.ContentEncoding = Encoding.UTF8;

            var buffer = Encoding.UTF8.GetBytes(response);
            context.Response.ContentLength64 = buffer.Length;
            context.Response.OutputStream.Write(buffer, 0, buffer.Length);
            context.Response.OutputStream.Close();
        }

        private void ContextCallback(IAsyncResult result)
        {
            var context = _listener.EndGetContext(result);           
            
            // continue next request
            _listener.BeginGetContext(ContextCallback, null);

            try
            {
                Trace.WriteLine(context.Request.HttpMethod + " '" + context.Request.RawUrl
                    + "' from " + context.Request.RemoteEndPoint
                    + " using " + context.Request.UserAgent
                    + "");
                context.Response.KeepAlive = true;

                // deal with current request
                var path = context.Request.RawUrl;
                var query = "";
                var queryStart = path.IndexOf('?');
                if (queryStart > 0)
                {
                    path = path.Substring(0, queryStart);
                    if (queryStart < path.Length - 1)
                    {
                        query = path.Substring(queryStart + 1);
                    }
                }

                string prefix;
                var handler = GetHandler(path, out prefix);
                Trace.Assert(null != handler);

                path = path.Substring(prefix.Length);
                handler(context, path, query);
            }
            catch (Exception e)
            {
                Trace.TraceError("HttpContextCallback throws exception = " + e + ", StackTrace = " + e.StackTrace);
            }
        }

        private void RegisterHandler(string prefix, HttpRequestHandler handler)
        {
            _handlers.Add(prefix.ToLower(), handler);
        }

        private HttpRequestHandler GetHandler(string path, out string oprefix)
        {
            var matchCount = -1;
            var matchIndex = -1;
            var max = _sortedHandlerArrary.Length;
            oprefix = "";
            path = path.ToLower();

            for (var i = 0; i < max; i++)
            {
                var prefix = _sortedHandlerArrary[i].Key;
                var localMatchCount = 0;

                for (var j = 0; j < prefix.Length && j < path.Length; j++)
                {
                    if (prefix[j] == path[j])
                    {
                        localMatchCount = j + 1;
                    }
                    else
                        break;
                }

                // continue if 
                if (localMatchCount >= matchCount)
                {
                    matchCount = localMatchCount;

                    if (localMatchCount == prefix.Length)
                    {
                        matchIndex = i;
                        oprefix = prefix;
                    }
                }

                // break
                else
                {
                    break;
                }
            }

            if (matchIndex != -1)
            {
                return _sortedHandlerArrary[matchIndex].Value;
            }
            oprefix = "NotFound";
            return Http404NotFoundHandler;
        }

        protected delegate void HttpRequestHandler(HttpListenerContext context, string subpath, string query);
        
        private int _port;
        private HttpListener _listener;
        private SortedDictionary<string, HttpRequestHandler> _handlers = new SortedDictionary<string, HttpRequestHandler>();
        private KeyValuePair<string, HttpRequestHandler>[] _sortedHandlerArrary;
    }
}
