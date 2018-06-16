/*
 * Copyright 2017, TopicQuests
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.topicquests.backside.kafka.apps.chat;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.*;


/**
 * @author jackpark
 * A simple Frame for doing chats
 */
public class ChatUI {
	private SimpleChatApp app;
	private JFrame frame;
	private JTextArea chatArea = new JTextArea();
	private JTextField chatField = new JTextField();
	private JButton sendButton = new JButton("Send");
	
	/**
	 * 
	 */
	public ChatUI() {
		buildGUI();
		app = new SimpleChatApp(this);
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() { 
		    	app.close();
		    }
		});
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new ChatUI();
	}

	void buildGUI() {
		JMenuBar jMenuBar1 = new JMenuBar();
		JMenu jMenuFile = new JMenu();
		JMenuItem jMenuFileExit = new JMenuItem();
		frame = new JFrame();
		frame.setSize(400, 400);
		frame.setTitle("ChatApp");
		JPanel contentPane = (JPanel)frame.getContentPane();
		JPanel southPanel = new JPanel(new BorderLayout());
		contentPane.setLayout(new BorderLayout());
		JScrollPane consoleTab = new JScrollPane();
		contentPane.add(consoleTab, BorderLayout.CENTER);
		chatArea.setEditable(false);
	    chatArea.setText("");
	    chatArea.setLineWrap(true);
	    consoleTab.getViewport().add(chatArea);
	    southPanel.add(chatField, BorderLayout.CENTER);
	    southPanel.add(sendButton, BorderLayout.EAST);
	    sendButton.addActionListener(new ActionListener() {
	         public void actionPerformed(ActionEvent e) {
	             String txt = chatField.getText();
	             System.out.println("GOT "+txt);
	             if (!txt.equals("")) {
	            	 app.sendMessage(txt);
	            	 chatField.setText("");
	            	 //don't have to echo this to the view
	            	 // since it will be picked up by the consumer
	             }
	          }          
	       });
	    contentPane.add(southPanel, BorderLayout.SOUTH);
		jMenuFileExit.setText("Exit");
	    jMenuFileExit.addActionListener(new MainFrame_jMenuFileExit_ActionAdapter(this));
	    jMenuFile.add(jMenuFileExit);
	    jMenuBar1.add(jMenuFile);
	    frame.setJMenuBar(jMenuBar1);
	    frame.setVisible(true);
	}
	
	public void say(String text) {
		chatArea.append(text+"\n");
	}
	
	void jMenuFileExit_actionPerformed(ActionEvent actionEvent) {
		app.close();
		System.exit(0);
	}
	
}

class MainFrame_jMenuFileExit_ActionAdapter
	implements ActionListener {
	ChatUI adaptee;

	MainFrame_jMenuFileExit_ActionAdapter(ChatUI adaptee) {
		this.adaptee = adaptee;
	}

	public void actionPerformed(ActionEvent actionEvent) {
		adaptee.jMenuFileExit_actionPerformed(actionEvent);
	}
}
