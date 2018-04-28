/**
 * London Calculator
 * (C) 2010 Fabrizio Fazzino
 * Released under the GPL License v3.0
 */

package it.fazzino;


import java.awt.*;
import javax.swing.*;
import javax.swing.event.*;

public class LondonCalculator extends JFrame implements ChangeListener {
	
	// Customizable values
	private int salaryPerYear = 60;		// Initial salary
	final int costOfRent = 1250;		// Cost of house rent

	// Graphical elements that must be accessed also outside the constructor
	JSlider salarySlider;
	JLabel takehomeAmountLabel;
	JLabel availableAmountLabel;
	
	// The constructor just plays with graphical elements
	public LondonCalculator() {
		
		// Create the main window
		super(new String("London Calculator"));
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		setSize(800,600);
		setLocationRelativeTo(null);
		setIconImage(new ImageIcon(getClass().getResource("images/tube-icon.jpg")).getImage());
		
		// Prepare the main panel
		JPanel mainPanel = new JPanel(new GridLayout(1,2));
		JPanel leftPanel = new JPanel(new GridLayout(1,1));
		JPanel rightPanel = new JPanel(new GridLayout(8,1));
		mainPanel.add(leftPanel);
		mainPanel.add(rightPanel);

		// Add image to the left panel
		ImageIcon gherkinIcon = new ImageIcon(getClass().getResource("images/gherkin.jpg"));
		JLabel gherkinLabel = new JLabel();
		gherkinLabel.setIcon(gherkinIcon);
		leftPanel.add(gherkinLabel);
		
		// Add controls to the right panel
		JLabel salaryCaptionLabel = new JLabel("Base Salary [KiloGBP/year]:", JLabel.CENTER);
		salaryCaptionLabel.setFont(new Font("Arial", Font.BOLD, 18));
		rightPanel.add(salaryCaptionLabel);
		
		salarySlider = new JSlider(40,100,salaryPerYear);
		salarySlider.setMinorTickSpacing(5);
		salarySlider.setMajorTickSpacing(10);
		salarySlider.createStandardLabels(5);
		salarySlider.setSnapToTicks(true);
		salarySlider.setPaintTicks(true);
		salarySlider.setPaintLabels(true);
		salarySlider.addChangeListener(this);
		rightPanel.add(salarySlider);
				
		JLabel takehomeCaptionLabel = new JLabel("Take home:", JLabel.CENTER);
		takehomeCaptionLabel.setFont(new Font("Arial", Font.BOLD, 18));
		rightPanel.add(takehomeCaptionLabel);

		takehomeAmountLabel = new JLabel("", JLabel.CENTER);
		takehomeAmountLabel.setFont(new Font("Courier", Font.BOLD, 24));
		takehomeAmountLabel.setForeground(Color.red);
		rightPanel.add(takehomeAmountLabel);

		JLabel rentCaptionLabel = new JLabel("Rent of a typical 2 bedrooms house:", JLabel.CENTER);
		rentCaptionLabel.setFont(new Font("Arial", Font.BOLD, 18));
		rightPanel.add(rentCaptionLabel);

		JLabel rentAmountLabel = new JLabel(Integer.toString(costOfRent)+" [GBP/month]", JLabel.CENTER);
		rentAmountLabel.setFont(new Font("Courier", Font.BOLD, 24));
		rentAmountLabel.setForeground(Color.blue);
		rightPanel.add(rentAmountLabel);

		JLabel availableCaptionLabel = new JLabel("Available net salary:", JLabel.CENTER);
		availableCaptionLabel.setFont(new Font("Arial", Font.BOLD, 18));
		rightPanel.add(availableCaptionLabel);

		availableAmountLabel = new JLabel("", JLabel.CENTER);
		availableAmountLabel.setFont(new Font("Courier", Font.BOLD, 24));
		availableAmountLabel.setForeground(Color.red);
		rightPanel.add(availableAmountLabel);
		
		// Make the window visible
		getContentPane().add(mainPanel);
		setVisible(true);
		repaint();
	}
	
	// This method is called every time you move the slider
	public void stateChanged(ChangeEvent e) {
		if(e.getSource().equals(salarySlider) && !salarySlider.getValueIsAdjusting()) {
            salaryPerYear = salarySlider.getValue();
            repaint();
        }
	}
	
	// The repaint() at the end of the previous method will call this one
	public void paint(Graphics g) {
		int salaryPerMonth = salaryPerYear*1000/20+430;
		takehomeAmountLabel.setText(Integer.toString(salaryPerMonth)+" [GBP/month]");
		availableAmountLabel.setText(Integer.toString(salaryPerMonth-costOfRent)+" [GBP/m] = "+
				Integer.toString(new Double((salaryPerMonth-costOfRent)*1.15).intValue())+" [EUR/m]");
		super.paint(g);
	}

	// The main function just creates an instance of the class
	public static void main(String[] args) {
		LondonCalculator lc = new LondonCalculator();
	}

}
