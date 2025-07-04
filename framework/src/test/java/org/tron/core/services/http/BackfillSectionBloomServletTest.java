package org.tron.core.services.http;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.store.SectionBloomStore;
import org.tron.core.store.TransactionRetStore;

@RunWith(MockitoJUnitRunner.class)
public class BackfillSectionBloomServletTest {

  @Mock
  private ChainBaseManager chainBaseManager;

  @Mock
  private SectionBloomStore sectionBloomStore;

  @Mock
  private TransactionRetStore transactionRetStore;

  @InjectMocks
  private BackfillSectionBloomServlet servlet;

  private HttpServletRequest request;
  private HttpServletResponse response;
  private StringWriter responseWriter;

  @Before
  public void setUp() throws IOException {
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    responseWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(responseWriter);
    when(response.getWriter()).thenReturn(printWriter);

    when(chainBaseManager.getSectionBloomStore()).thenReturn(sectionBloomStore);
    when(chainBaseManager.getTransactionRetStore()).thenReturn(transactionRetStore);
  }

  @Test
  public void testDoGetWithValidParameters() throws Exception {
    // 设置请求参数
    when(request.getParameter("startBlock")).thenReturn("1");
    when(request.getParameter("endBlock")).thenReturn("10");
    when(chainBaseManager.getHeadBlockNum()).thenReturn(100L);

    // 模拟区块和交易数据
    BlockCapsule mockBlock = mock(BlockCapsule.class);
    TransactionRetCapsule mockTransactionRet = mock(TransactionRetCapsule.class);

    when(chainBaseManager.getBlockByNum(anyLong())).thenReturn(mockBlock);
    when(transactionRetStore.getTransactionInfoByBlockNum(any())).thenReturn(mockTransactionRet);
    when(sectionBloomStore.initBlockSection(any())).thenReturn(null);

    servlet.doGet(request, response);

    // 验证响应不为空
    String responseContent = responseWriter.toString();
    assert !responseContent.isEmpty();
  }

  @Test
  public void testDoGetWithInvalidParameters() throws Exception {
    // 测试负数参数
    when(request.getParameter("startBlock")).thenReturn("-1");
    when(request.getParameter("endBlock")).thenReturn("10");

    servlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    assert responseContent.contains("Block numbers must be non-negative");
  }

  @Test
  public void testDoGetWithInvalidRange() throws Exception {
    // 测试起始块大于结束块
    when(request.getParameter("startBlock")).thenReturn("10");
    when(request.getParameter("endBlock")).thenReturn("5");

    servlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    assert responseContent.contains("Start block must be less than or equal to end block");
  }

  @Test
  public void testConcurrentRequestsRejection() throws Exception {
    // 模拟第一个请求正在处理
    when(request.getParameter("startBlock")).thenReturn("1");
    when(request.getParameter("endBlock")).thenReturn("100");
    when(chainBaseManager.getHeadBlockNum()).thenReturn(1000L);

    // 第一个请求应该成功开始
    servlet.doGet(request, response);

    // 重置response writer为第二个请求
    responseWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(responseWriter);
    when(response.getWriter()).thenReturn(printWriter);

    // 第二个请求应该被拒绝
    servlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    assert responseContent.contains("Another backfill request is already in progress");
  }

  @Test
  public void testDoGetWithBlocksExceedingHead() throws Exception {
    // 测试超出当前头块
    when(request.getParameter("startBlock")).thenReturn("100");
    when(request.getParameter("endBlock")).thenReturn("200");
    when(chainBaseManager.getHeadBlockNum()).thenReturn(50L);

    servlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    assert responseContent.contains("Block numbers cannot exceed current head block");
  }
}
