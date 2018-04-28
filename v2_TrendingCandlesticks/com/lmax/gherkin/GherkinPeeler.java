/**
 * Gherkin Peeler
 *
 * A simple scalper for the London Multi-Asset eXchange (LMAX).
 *
 * v2 - Trending Candlesticks
 */

package com.lmax.gherkin;

import java.util.List;
import com.lmax.api.*;
import com.lmax.api.account.*;
import com.lmax.api.account.LoginRequest.*;
import com.lmax.api.order.*;
import com.lmax.api.orderbook.*;
import com.lmax.api.reject.*;
import com.lmax.api.heartbeat.*;
import com.lmax.api.position.*;

public class GherkinPeeler implements LoginCallback, OrderBookEventListener, OrderEventListener, InstructionRejectedEventListener, ExecutionEventListener, StreamFailureListener, SessionDisconnectedListener, HeartbeatEventListener, PositionEventListener, Runnable
{

  /*
   * Constants
   */

  private static enum GherkinState {READY_TO_OPEN, WAIT_FOR_OPEN, READY_TO_CLOSE, WAIT_FOR_CLOSE};

  private static final short GOING_DUNNO = 0;
  private static final short GOING_DOWN  = 1;
  private static final short GOING_UP    = 2;

  private static final long instrumentId[] = {4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008, 4009, 4010, 4011, 4012, 4013, 4014, 4015, 4016, 4017};
  private static final String instrumentDesc[] = {"EUR/USD", "GBP/USD", "EUR/GBP", "USD/JPY", "GBP/JPY", "EUR/JPY", "AUD/USD", "AUD/JPY", "CHF/JPY", "USD/CHF", "EUR/CHF", "GBP/CHF", "USD/CAD", "GBP/CAD", "EUR/CAD", "EUR/AUD", "GBP/AUD"};
  private static final short numInstrumentsToTrade = (short)instrumentId.length;

  private static final long  orderQuantity = 10 * FixedPointNumber.ONE.longValue(); // May vary according to wallet size
  private static final short bookLevel = 0;             // Book level: 0=top, 4=max_depth_available
  private static final short consecutiveThreshold = 3;  // Minimum moves in the same direction necessary to trigger order
  private static final short spreadMultiplierToOpen = 1;
  private static final short spreadMultiplierToClose = 2;

  /*
   * Variables
   */

  private Session session;
  private GherkinState state;

  private static FixedPointNumber[] firstBid = new FixedPointNumber[numInstrumentsToTrade];
  private static FixedPointNumber[] firstAsk = new FixedPointNumber[numInstrumentsToTrade];

  private static FixedPointNumber[] lastBid = new FixedPointNumber[numInstrumentsToTrade];
  private static FixedPointNumber[] lastAsk = new FixedPointNumber[numInstrumentsToTrade];

  private static short[] lastDirection = new short[numInstrumentsToTrade];
  private static int[] lastDirectionCount = new int[numInstrumentsToTrade];

  private static FixedPointNumber[] lastBidQuantity = new FixedPointNumber[numInstrumentsToTrade];
  private static FixedPointNumber[] lastAskQuantity = new FixedPointNumber[numInstrumentsToTrade];
  private static FixedPointNumber[] lastSpread = new FixedPointNumber[numInstrumentsToTrade];
  private static FixedPointNumber[] maxSpread = new FixedPointNumber[numInstrumentsToTrade];
  private static FixedPointNumber[] minSpread = new FixedPointNumber[numInstrumentsToTrade];

  private static short outstandingPos;
  private static FixedPointNumber outstandingQuantity;
  private static FixedPointNumber closingPriceMin;
  private static FixedPointNumber closingPriceMax;

  /*
   * Constructor
   */

  GherkinPeeler(String url, String username, String password, ProductType productType) {
    LmaxApi lmaxApi = new LmaxApi(url);
    lmaxApi.login(new LoginRequest(username, password, productType), this);
  }

  /*
   * Overridden methods
   */

  @Override
  public void onLoginSuccess(Session session)
  {
    System.out.println("Login Success: " + session.getAccountDetails().getAccountId());

    // Save the session for later use.
    this.session = session;

    // Register event listeners and callbacks and start the session
    GherkinStart();
  }

  @Override
  public void onLoginFailure(FailureResponse failureResponse)
  {
    System.err.println("ERROR - Login Failure: " + failureResponse);
    GherkinStop();
    System.exit(-1);
  }

  @Override
  public void notify(OrderBookEvent orderBookEvent)
  {
    if (orderBookEvent.getBidPrices().size() == 0 || orderBookEvent.getAskPrices().size() == 0)
    {
      System.out.println("WARNING - Empty Order Book");
      return;
    }
    else if (orderBookEvent.getBidPrices().size() < 5 || orderBookEvent.getAskPrices().size() < 5)
    {
      System.out.println("WARNING - Poor Order Book");
      return;
    }

    /*
     * Always update prices and quantities
     */

    short updatedPos = numInstrumentsToTrade;
    final long id = orderBookEvent.getInstrumentId();
    for (short i=0; i<numInstrumentsToTrade; i++)
    {
      if (id == instrumentId[i]) updatedPos = i;
    }
    if (updatedPos == numInstrumentsToTrade)
    {
      System.err.println("ERROR - Unknown Instrument ID: " + id);
      return;
    }

    // Current prices
    FixedPointNumber thisBid = orderBookEvent.getBidPrices().get(bookLevel).getPrice();
    FixedPointNumber thisAsk = orderBookEvent.getAskPrices().get(bookLevel).getPrice();

    // First time only
    if (firstBid[updatedPos] == FixedPointNumber.ZERO || firstAsk[updatedPos] == FixedPointNumber.ZERO)
    {
      firstBid[updatedPos] = thisBid;
      firstAsk[updatedPos] = thisAsk;
      lastBid[updatedPos] = thisBid;
      lastAsk[updatedPos] = thisAsk;
      return;
    }

    // Update direction
    short thisDirection;
    if (thisBid.longValue() > lastBid[updatedPos].longValue() && thisAsk.longValue() > lastAsk[updatedPos].longValue())
      thisDirection = GOING_UP;
    else if (thisBid.longValue() < lastBid[updatedPos].longValue() && thisAsk.longValue() < lastAsk[updatedPos].longValue())
      thisDirection = GOING_DOWN;
    else
      thisDirection = GOING_DUNNO;

    // Change of direction, reset initial value
    if (thisDirection == GOING_DUNNO || thisDirection != lastDirection[updatedPos])
    {
      lastDirection[updatedPos] = thisDirection;
      lastDirectionCount[updatedPos] = 0;
      firstBid[updatedPos] = thisBid;
      firstAsk[updatedPos] = thisAsk;
    }
    else lastDirectionCount[updatedPos]++;

    // Update prices and quantities
    lastBid[updatedPos] = thisBid;
    lastAsk[updatedPos] = thisAsk;
    lastBidQuantity[updatedPos] = orderBookEvent.getBidPrices().get(bookLevel).getQuantity();
    lastAskQuantity[updatedPos] = orderBookEvent.getAskPrices().get(bookLevel).getQuantity();

    // Also update the maximum and minumum spread to use the average value in order to avoid false triggers
    long currentSpread = lastAsk[updatedPos].longValue() - lastBid[updatedPos].longValue();
    lastSpread[updatedPos] = FixedPointNumber.valueOf(currentSpread);
    if (currentSpread > maxSpread[updatedPos].longValue())
    {
      maxSpread[updatedPos] = FixedPointNumber.valueOf(currentSpread);
    }
    if (currentSpread < minSpread[updatedPos].longValue())
    {
      minSpread[updatedPos] = FixedPointNumber.valueOf(currentSpread);
    }

    /*
     * Now check if there are orders to place
     */
    switch (state)
    {
      case READY_TO_OPEN:
      {
        /*
         * Apply the trading idea
         */

        long multipleSpreadToOpen = spreadMultiplierToOpen * (minSpread[updatedPos].longValue() + maxSpread[updatedPos].longValue());
        this.outstandingQuantity = FixedPointNumber.ZERO;

        /*
         * Are we going up?
         */

        if (lastDirection[updatedPos] == GOING_UP &&
            lastDirectionCount[updatedPos] >= consecutiveThreshold &&
            lastBid[updatedPos].longValue() > firstAsk[updatedPos].longValue() + multipleSpreadToOpen)
        {
          this.outstandingPos = updatedPos;
          this.outstandingQuantity = FixedPointNumber.valueOf(orderQuantity);  // Positive
          System.out.println(instrumentDesc[updatedPos] + " @ " + thisBid + "/" + thisAsk + " trending up, open with buy around " + thisAsk);
        }
        else

        /*
         * Are we going down?
         */

        if (lastDirection[updatedPos] == GOING_DOWN &&
            lastDirectionCount[updatedPos] >= consecutiveThreshold &&
            lastAsk[updatedPos].longValue() < firstBid[updatedPos].longValue() - multipleSpreadToOpen)
        {
          this.outstandingPos = updatedPos;
          this.outstandingQuantity = FixedPointNumber.valueOf(orderQuantity).negate();  // Negative
          System.out.println(instrumentDesc[updatedPos] + " @ " + thisBid + "/" + thisAsk + " trending down, open with sell around " + thisBid);
        }
        else this.outstandingQuantity = FixedPointNumber.ZERO;

        /*
         * Place order to open position
         */

        if (this.outstandingQuantity != FixedPointNumber.ZERO)
        {
          MarketOrderSpecification order = new MarketOrderSpecification(instrumentId[this.outstandingPos], this.outstandingQuantity, TimeInForce.FILL_OR_KILL);
          System.out.println("Try to place open order...");
          session.placeMarketOrder(order, new GherkinOrderCallback()
          {
            @Override
            public void onSuccess(long instructionId)
            {
              System.out.println("Open Order placed successfully!");
              state = GherkinState.WAIT_FOR_OPEN;
            }
            @Override
            public void onFailure(FailureResponse failureResponse)
            {
              System.err.println("ERROR - Open Order Failure: " + failureResponse);
              throw new RuntimeException("Runtime Exception: Open Order Failure");
            }
          });
        }

        // This is entirely optional, explain why we didn't open
        else if (lastDirectionCount[updatedPos] >= consecutiveThreshold)
        {
          if (lastDirection[updatedPos] == GOING_UP)
          {
            System.out.println("Not betting on " + instrumentDesc[updatedPos] + " @ " + lastBid[updatedPos] + "/" + lastAsk[updatedPos] + " despite trending up " + lastDirectionCount[updatedPos] + " times because " + lastBid[updatedPos] + " is not > " + FixedPointNumber.valueOf(firstAsk[updatedPos].longValue() + multipleSpreadToOpen));
          }
          else if (lastDirection[updatedPos] == GOING_DOWN)
          {
            System.out.println("Not betting on " + instrumentDesc[updatedPos] + " @ " + lastBid[updatedPos] + "/" + lastAsk[updatedPos] + " despite trending down " + lastDirectionCount[updatedPos] + " times because " + lastAsk[updatedPos] + " is not < " + FixedPointNumber.valueOf(firstBid[updatedPos].longValue() - multipleSpreadToOpen));
          }
        }
        // end of optional part

        break;
      }
      case WAIT_FOR_OPEN:
      {
        System.out.println("Waiting for Open Order to be Processed");
        break;
      }
      case READY_TO_CLOSE:
      {
        boolean closeOrder = false;
        if (updatedPos == this.outstandingPos)
        {
          if (this.outstandingQuantity.longValue() > 0)
          {
            // Buy to sell, best case
            if (lastBid[this.outstandingPos].longValue() > this.closingPriceMax.longValue())
            {
              closeOrder = true;
              System.out.println("Close " + instrumentDesc[this.outstandingPos] + " with sell @ " + lastBid[this.outstandingPos] + " (BEST)");
            }
            else
            // Buy to sell, worst case
            if (lastBid[this.outstandingPos].longValue() < this.closingPriceMin.longValue())
            {
              closeOrder = true;
              System.out.println("Close " + instrumentDesc[this.outstandingPos] + " with sell @ " + lastBid[this.outstandingPos] + " (WORST)");
            }
          }
          else if (this.outstandingQuantity.longValue() < 0)
          {
            // Sell to buy, best case
            if (lastAsk[this.outstandingPos].longValue() < this.closingPriceMin.longValue())
            {
              closeOrder = true;
              System.out.println("Close " + instrumentDesc[this.outstandingPos] + " with buy @ " + lastAsk[this.outstandingPos] + " (BEST)");
            }
            else
            // Sell to buy, worst case
            if (lastAsk[this.outstandingPos].longValue() > this.closingPriceMax.longValue())
            {
              closeOrder = true;
              System.out.println("Close " + instrumentDesc[this.outstandingPos] + " with buy @ " + lastAsk[this.outstandingPos] + " (WORST)");
            }
          }
          else
          {
            System.err.println("ERROR - Zero quantity detected");
            break;
          }
          if (closeOrder)
          {
            // Place closing order
            MarketOrderSpecification order = new MarketOrderSpecification(instrumentId[this.outstandingPos], this.outstandingQuantity.negate(), TimeInForce.FILL_OR_KILL);
            System.out.println("Place Close Order: " + order);
            session.placeMarketOrder(order, new GherkinOrderCallback()
            {
              @Override
              public void onSuccess(long instructionId)
              {
                System.out.println("Close Order Success: " + instructionId);
                state = GherkinState.WAIT_FOR_CLOSE;
              }
            });
          }
          else
          {
            if (this.outstandingQuantity.longValue() > 0)
            {
              System.out.println("Not closing now with sell, waiting for " + instrumentDesc[this.outstandingPos] + " with bid now @ " + lastBid[updatedPos] + " to exit range WORST/BEST " + this.closingPriceMin + "/" + this.closingPriceMax);
            }
            else if (this.outstandingQuantity.longValue() < 0)
            {
              System.out.println("Not closing now with buy, waiting for " + instrumentDesc[this.outstandingPos] + " with ask now @ " + lastAsk[updatedPos] + " to exit range BEST/WORST " + this.closingPriceMin + "/" + this.closingPriceMax);
            }
          }
        }
        break;
      }
      case WAIT_FOR_CLOSE:
      {
        System.out.println("Waiting for Close Order to be Processed");
        break;
      }
      default:
      {
        System.err.println("ERROR - Unknown State: " + state);
      }
    }  // switch (state)
  }

  @Override
  public void notify(InstructionRejectedEvent instructionRejected)
  {
    final long instructionId = instructionRejected.getInstructionId();
    System.err.println("ERROR - Instruction Rejected: " + instructionId);
  }

  /*
   * The order event will always arrive before the associated execution events for that order.
   */
  @Override
  public void notify(Order order)
  {
    System.out.println("Order Notify: " + order);
  }

  @Override
  public void notify(Execution execution)
  {
    System.out.println("Execution Notify: " + execution);
    switch (state)
    {
      case WAIT_FOR_OPEN:
      {
        this.outstandingQuantity = execution.getOrder().getFilledQuantity();
        long multipleSpreadToClose = spreadMultiplierToClose * (minSpread[this.outstandingPos].longValue() + maxSpread[this.outstandingPos].longValue());
        if (this.outstandingQuantity.longValue() > 0)
        {
          this.closingPriceMax = FixedPointNumber.valueOf(execution.getPrice().longValue() + multipleSpreadToClose);
          this.closingPriceMin = FixedPointNumber.valueOf(lastBid[this.outstandingPos].longValue() - multipleSpreadToClose);
        }
        else
        {
          this.closingPriceMax = FixedPointNumber.valueOf(lastAsk[this.outstandingPos].longValue() + multipleSpreadToClose);
          this.closingPriceMin = FixedPointNumber.valueOf(execution.getPrice().longValue() - multipleSpreadToClose);
        }
        System.out.println("Ready to close outside range " + this.closingPriceMin + "/" + this.closingPriceMax);
        state = GherkinState.READY_TO_CLOSE;
        break;
      }
      case WAIT_FOR_CLOSE:
      {
        this.outstandingQuantity = FixedPointNumber.valueOf(this.outstandingQuantity.longValue() + execution.getOrder().getFilledQuantity().longValue());
        if (this.outstandingQuantity.longValue() == 0)
        {
          System.out.println("Ready to open again");
          state = GherkinState.READY_TO_OPEN;
        }
        else
        {
          System.out.println("Modified Outstanding Quantity: " + this.outstandingQuantity);
        }
        break;
      }
      default:
      {
        System.err.println("ERROR - Execution Notify with Unexpected State");
      }
    }
  }

  @Override
  public void notifyStreamFailure(Exception e)
  {
    System.err.println("ERROR - Stream Exception: " + e);
    GherkinStop();
    GherkinStart();
  }

  @Override
  public void notifySessionDisconnected()
  {
    System.err.println("ERROR - Session Disconnected, No Heartbeat");
    GherkinStop();
    GherkinStart();
  }

  @Override
  public void notify(long accountId, String token)
  {
    System.out.printf("Heartbeat Received: %d, %s%n", accountId, token);
  }

  @Override
  public void notify(final PositionEvent position)
  {
    System.out.println("Notify Position: " + position);

    if (position.getOpenQuantity().longValue() != 0 && state == GherkinState.READY_TO_OPEN)
    {
      System.out.printf("WARNING - Unexpected Open Position, Closing %s x %d\n", position.getOpenQuantity(), position.getInstrumentId());
      MarketOrderSpecification order = new MarketOrderSpecification(position.getInstrumentId(), position.getOpenQuantity().negate(), TimeInForce.IMMEDIATE_OR_CANCEL);
      session.placeMarketOrder(order, new GherkinOrderCallback()
      {
        @Override
        public void onSuccess(long instructionId)
        {
          System.out.println("Close Position Success: " + instructionId);
        }
      });
    }
  }

  @Override
  public void run()
  {
    try
    {
      while (!Thread.currentThread().isInterrupted())
      {
        Thread.sleep(300000L);  // 5 min => msec
        session.requestHeartbeat(new HeartbeatRequest("token"), new HeartbeatCallback()
        {
          @Override
          public void onSuccess(String token)
          {
            System.out.println("Heartbeat Requested: " + token);
          }
            
          @Override
          public void onFailure(FailureResponse failureResponse)
          {
            throw new RuntimeException("Runtime Exception: Heartbeat Request Failure");
          }
        });
      }
    }
    catch (Exception e)
    {
      System.err.println("ERROR - Runnable Interrupted Exception: " + e);
      GherkinStop();
      GherkinStart();
    }
  }

  /*
   * New methods
   */

  private void GherkinStart()
  {
    // Reset variables to their initial values
    state = GherkinState.READY_TO_OPEN;
    for (short i=0; i<numInstrumentsToTrade; i++)
    {
      firstBid[i] = FixedPointNumber.ZERO;
      firstAsk[i] = FixedPointNumber.ZERO;

      lastBid[i] = FixedPointNumber.ZERO;
      lastAsk[i] = FixedPointNumber.ZERO;

      lastDirection[i] = GOING_DUNNO;
      lastDirectionCount[i] = 0;

      lastBidQuantity[i] = FixedPointNumber.ZERO;
      lastAskQuantity[i] = FixedPointNumber.ZERO;

      maxSpread[i] = FixedPointNumber.ZERO;
      minSpread[i] = FixedPointNumber.ONE;
    }

    // Add listeners and callbacks for all the events I am interested in
    session.registerOrderBookEventListener(this);
    session.registerInstructionRejectedEventListener(this);
    session.registerOrderEventListener(this);
    session.registerExecutionEventListener(this);
    session.registerStreamFailureListener(this);
    session.registerSessionDisconnectedListener(this);
    session.registerPositionEventListener(this);
    session.subscribe(new OrderSubscriptionRequest(), new GherkinSubscriptionCallback());
    for(int i = 0; i < numInstrumentsToTrade; i++)
    {
      session.subscribe(new OrderBookSubscriptionRequest(instrumentId[i]), new GherkinSubscriptionCallback());
    }
    session.registerHeartbeatListener(this);
    session.subscribe(new HeartbeatSubscriptionRequest(), new Callback()
    {
      @Override
      public void onSuccess()
      {
        System.out.println("Heartbeat Subscription Success");
      }

      @Override
      public void onFailure(final FailureResponse failureResponse)
      {
        throw new RuntimeException("Runtime Exception: Heartbeat Subscription Failure");
      }
    });
    new Thread(this).start();

    // Start the event processing loop, this method will block until the session is stopped.
    session.start();
  }

  private void GherkinStop()
  {
    session.stop();
  }

  /*
   * Nested classes containing callbacks
   */

  private abstract class GherkinOrderCallback implements OrderCallback
  {
    @Override
    public void onFailure(FailureResponse failureResponse)
    {
      System.err.println("ERROR - Order Callback Failure: " + failureResponse);
      if (!failureResponse.isSystemFailure())
      {
        System.err.printf("ERROR - Order Callback Data Error: Message: %s, Description: %s", failureResponse.getMessage(), failureResponse.getDescription());
      }
      else
      {
        Exception e = failureResponse.getException();
        if (null != e)
        {
          System.err.println("ERROR - Order Callback Exception Stacktrace: ");
          e.printStackTrace();
        }
        else
        {
          System.err.printf("ERROR - Order Callback Error: Message: %s, Description: %s\n", failureResponse.getMessage(), failureResponse.getDescription());
        }
      }
      GherkinStop();
      GherkinStart();
    }
  }

  private final class GherkinSubscriptionCallback implements Callback
  {
    @Override
    public void onSuccess()
    {
      System.out.println("Subscription Callback Success");
    }

    @Override
    public void onFailure(FailureResponse failureResponse)
    {
      System.err.println("ERROR - Subscription Callback Failure: " + failureResponse);
      GherkinStop();
      System.exit(-1);
    }
  }

  /*
   * Main function
   */

  public static void main(String[] args)
  {
    if (args.length != 4)
    {
      System.out.println("Usage:");
      System.out.println("  " + GherkinPeeler.class.getName() + " <url> <username> <password> [CFD_DEMO|CFD_LIVE]");
      System.exit(-1);
    }

    String url = args[0];
    String username = args[1];
    String password = args[2];
    ProductType productType = ProductType.valueOf(args[3].toUpperCase());

    GherkinPeeler gherkinPeeler = new GherkinPeeler(url, username, password, productType);
  }

}


